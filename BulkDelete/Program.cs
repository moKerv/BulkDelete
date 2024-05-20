using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.PowerPlatform.Dataverse.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    private static int _recordDeletedCount = 0;
    private static int _totalRecords = 0;
    private static DateTime _startTime;
    private static int _batchRuns = 0;
    private static int _processingRetry, _allRetries = 0;
    private static Dictionary<string, int> _reportDictionary = new Dictionary<string, int>();
    private static ConcurrentDictionary<string, ServiceClient> connections = new ConcurrentDictionary<string, ServiceClient>();
    private static readonly object fileLock = new object();
    private static int _batchesStarted, _batchesEnded, _batchesFailed = 0;

    static async Task Main(string[] args)
    {
        Console.Write("Enter the entity name: ");
        string entityName = Console.ReadLine();

        Console.Write("Enter the batch size: ");
        if (!int.TryParse(Console.ReadLine(), out int batchSize))
        {
            Console.WriteLine("Invalid batch size. Using the default value (100).");
            batchSize = 100;
        }

        var clientConfig = ConfigurationManager.AppSettings.Get("ClientConfig");
        var clientPairs = JsonConvert.DeserializeObject<List<ClientConfig>>(clientConfig);

        string d365Url = ConfigurationManager.AppSettings.Get("D365URL");
        int threadCount = int.Parse(ConfigurationManager.AppSettings.Get("ThreadCount"));
        _allRetries = int.Parse(ConfigurationManager.AppSettings.Get("ProcessingRetries"));
        _processingRetry = 1;

        while (_processingRetry <= _allRetries)
        {
            Console.WriteLine("Getting All Records");

            var recordIds = FetchAllRecordIds(entityName, clientPairs.First(), d365Url);
            _totalRecords = recordIds.Count;
            _startTime = DateTime.Now;

            await ProcessBatchesAsync(entityName, batchSize, clientPairs, d365Url, threadCount, recordIds);
            _processingRetry++;
        }

        Console.WriteLine("Press Enter to exit.");
        Console.ReadLine();
    }

    private static async Task ProcessBatchesAsync(string entityName, int batchSize, List<ClientConfig> clientPairs, string d365Url, int threadCount, List<Guid> recordIds)
    {
        var sublists = SplitList(recordIds, clientPairs.Count);
        var tasks = new List<Task>();

        var semaphore = new SemaphoreSlim(threadCount);

        for (int i = 0; i < clientPairs.Count; i++)
        {
            var sublist = sublists[i];
            var clientPair = clientPairs[i];
            var recordBatches = SplitIntoBatches(sublist, batchSize);

            foreach (var batch in recordBatches)
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await ProcessBatchAsync(entityName, batch, clientPair, d365Url);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }
        }

        await Task.WhenAll(tasks);
    }

    private static List<List<Guid>> SplitList(List<Guid> list, int parts)
    {
        int chunkSize = list.Count / parts;
        int remainder = list.Count % parts;

        var sublists = new List<List<Guid>>();
        int currentIndex = 0;

        for (int i = 0; i < parts; i++)
        {
            var sublistSize = chunkSize + (i < remainder ? 1 : 0);
            sublists.Add(list.Skip(currentIndex).Take(sublistSize).ToList());
            currentIndex += sublistSize;
        }

        return sublists;
    }

    private static List<List<Guid>> SplitIntoBatches(List<Guid> source, int batchSize)
    {
        var batches = new List<List<Guid>>();
        for (int i = 0; i < source.Count; i += batchSize)
        {
            var batch = source.Skip(i).Take(batchSize).ToList();
            batches.Add(batch);
        }
        return batches;
    }

    private static List<Guid> FetchAllRecordIds(string entityName, ClientConfig client, string d365Url)
    {
        var connStr = $"AuthType=ClientSecret;url={d365Url};ClientId={client.ClientId};ClientSecret={client.ClientSecret}";
        using (var conn = new ServiceClient(connStr))
        {
            if (!conn.IsReady)
            {
                Console.WriteLine(conn.LastError);
                return new List<Guid>();
            }

            var allRecordIds = new List<Guid>();
            int pageNumber = 1;
            string pagingCookie = null;
            int fetchCount = 5000; // The page size

            while (true)
            {
                var query = new QueryExpression(entityName);
                query.ColumnSet = new ColumnSet(entityName + "id");
                query.PageInfo = new PagingInfo
                {
                    PageNumber = pageNumber,
                    Count = fetchCount,
                    PagingCookie = pagingCookie
                };

                var result = conn.RetrieveMultiple(query);

                allRecordIds.AddRange(result.Entities.Select(e => e.Id));
                Console.Write($"\rFetched: {allRecordIds.Count} records               ");
                _reportDictionary["ReportingLine"] = Console.CursorTop+5;
                if (result.MoreRecords)
                {
                    pagingCookie = result.PagingCookie;
                    pageNumber++;
                }
                else
                {
                    Console.WriteLine();
                    break;
                }
            }

            return allRecordIds;
        }
    }

    private static async Task ProcessBatchAsync(string entityName, List<Guid> batchRecordIds, ClientConfig clientPair, string d365Url)
    {
        Interlocked.Increment(ref _batchesStarted);

        ServiceClient conn = connections.GetOrAdd(clientPair.ClientId, clientId =>
        {
            var connStr = $"AuthType=ClientSecret;url={d365Url};ClientId={clientPair.ClientId};ClientSecret={clientPair.ClientSecret}";
            return new ServiceClient(connStr);
        });

        if (!conn.IsReady)
        {
            Interlocked.Increment(ref _batchesFailed);
            LogError("Batch Creation Error", conn.LastError);
            return;
        }

        try
        {
            var requests = new OrganizationRequestCollection();
            foreach (var recordId in batchRecordIds)
            {
                var deleteRequest = new DeleteRequest
                {
                    Target = new EntityReference(entityName, recordId)
                };
                deleteRequest.Parameters.Add("BypassCustomPluginExecution", true);
                requests.Add(deleteRequest);
            }

            var multipleRequest = new ExecuteMultipleRequest
            {
                Requests = requests,
                Settings = new ExecuteMultipleSettings
                {
                    ContinueOnError = true,
                    ReturnResponses = true
                }
            };

            var multipleResponse = (ExecuteMultipleResponse)await conn.ExecuteAsync(multipleRequest);

            foreach (var response in multipleResponse.Responses)
            {
                if (response.Fault != null)
                {
                    LogError("Delete Fault", $"Record ID: {batchRecordIds[response.RequestIndex]}, Error: {response.Fault.Message}");
                }
                else
                {
                    Interlocked.Increment(ref _recordDeletedCount);
                }
            }

            double percentComplete = (double)_recordDeletedCount / _totalRecords * 100;
            Interlocked.Increment(ref _batchRuns);
            TimeSpan elapsedTime = DateTime.Now.Subtract(_startTime);
            double timePerPercent = elapsedTime.TotalMilliseconds / percentComplete;
            double remainingPercent = 100 - percentComplete;

            string formattedTime = $"{elapsedTime.Hours:D2}:{elapsedTime.Minutes:D2}:{elapsedTime.Seconds:D2}";

            int cursorPosition = _reportDictionary["ReportingLine"];
            lock (fileLock)
            {
                Console.SetCursorPosition(0, cursorPosition);

                if (percentComplete > 0.001)
                {
                    TimeSpan estimatedTimeRemaining = TimeSpan.FromMilliseconds(timePerPercent * remainingPercent);
                    DateTime estimatedCompletionTime = DateTime.Now.Add(estimatedTimeRemaining);
                    int processingRate = (int)(_recordDeletedCount / elapsedTime.TotalSeconds);
                    Console.WriteLine($"      ------------------ RUN Number: {_processingRetry}/{_allRetries}".PadRight(79, '-'));
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Number of Records Processed: {_recordDeletedCount} of {_totalRecords}".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Percentage Complete          {percentComplete:F2}%".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Remaining Records:           {_totalRecords - _recordDeletedCount}".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Elapsed Time:                {formattedTime}".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Remaining Time:              {estimatedTimeRemaining}".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Transfer rate:               {processingRate} records per second".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Estimated Completion time:   {estimatedCompletionTime}".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine("      ----------------------------".PadRight(79, '-'));
                }

                Console.SetCursorPosition(0, cursorPosition + 12);
                Console.Write($"batches started: {_batchesStarted}, Batches completed: {_batchesEnded}, Batches failed: {_batchesFailed}");
            }

            Interlocked.Increment(ref _batchesEnded);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _batchesFailed);
            LogError("Process Batch Error", ex.Message);
        }
    }

    private class ClientConfig
    {
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
    }

    private static void LogError(string category, string message)
    {
        lock (fileLock)
        {
            using (StreamWriter writer = File.AppendText("error.log"))
            {
                writer.WriteLine($"{DateTime.Now} - {category}: {message}");
            }
        }
    }
}
