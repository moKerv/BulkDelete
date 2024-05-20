using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlTypes;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using System.Threading;
using Microsoft.Crm.Sdk.Messages;
using System.IO;
using Microsoft.PowerPlatform.Dataverse.Client;
using System.Collections.Concurrent;

class Program
{
    private static int _recorddeletedcount = 0;
    private static int _totalRecords = 0;
    private static DateTime _startTime;
    private static int _batchruns  = 0;
    private static int _processingRetry,_allretries = 0;
    private static Dictionary<string, int> _reportDictionary = new Dictionary<string, int>();
    static Dictionary<string, ServiceClient> connections = new Dictionary<string, ServiceClient>();
    private static readonly object fileLock = new object();
    private static int _batchesstarted, _batchesended, _batchesfailed = 0;
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

        // Fetch client IDs and secrets from the configuration in the specified format
        var clientConfig = ConfigurationManager.AppSettings.Get("ClientConfig");
        var clientPairs = JsonConvert.DeserializeObject<List<ClientConfig>>(clientConfig);

        // Fetch the D365 URL from the configuration
        string d365Url = ConfigurationManager.AppSettings.Get("D365URL");
        int ThreadCount = Convert.ToInt32(ConfigurationManager.AppSettings.Get("ThreadCount")); 
        _allretries = Convert.ToInt32(ConfigurationManager.AppSettings.Get("ProcessingRetries"));
        _processingRetry = 1;
          
        while (_processingRetry <= _allretries)
        {
            Console.WriteLine("Getting All Records");

            var recordIds = FetchAllRecordIds(entityName, clientPairs.First(), d365Url); // Example using the first client
            _totalRecords = recordIds.Count;
            _startTime = DateTime.Now;

            await ProcessBatchesAsync(batchSize, clientPairs, d365Url, ThreadCount, recordIds);
            _processingRetry++;
        }
        // Await user input before exiting
        Console.WriteLine("Press Enter to exit.");
        Console.ReadLine();
    }

    private static async Task ProcessBatchesAsync(int batchSize, List<ClientConfig> clientPairs, string d365Url, int threadCount, List<Guid> RecordIDs)
    {
        var sublists = SplitList(RecordIDs, clientPairs.Count);

        var tasks = new List<Task>();

        for (int i = 0; i < clientPairs.Count; i++)
        {
            var sublist = sublists[i];
            var clientPair = clientPairs[i];
            var semaphore = new SemaphoreSlim(threadCount);

            var recordBatches = SplitIntoBatches(sublist, threadCount);

            foreach (var batch in recordBatches)
            {
                await semaphore.WaitAsync();
                tasks.Add(ProcessBatchAsync("entityName", batch, clientPair, d365Url)
                    .ContinueWith(_ => semaphore.Release()));
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


    static List<Guid> FetchAllRecordIds(string entityName, ClientConfig client, string d365Url)
    {
        var connStr = $"AuthType=ClientSecret;url={d365Url};ClientId={client.ClientId};ClientSecret={client.ClientSecret}";
        using (var conn = new ServiceClient(connStr))
        {
            if (!conn.IsReady)
            {
                Console.WriteLine(conn.LastError);
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

                foreach (var entity in result.Entities)
                {
                    var recordId = entity.Id;
                    allRecordIds.Add(recordId);
                }
                Console.Write("\r Fetched: {0} records               ", allRecordIds.Count);
                _reportDictionary["ReportingLine"] = Console.CursorTop;
                if (result.MoreRecords)
                {
                    pagingCookie = result.PagingCookie;
                    pageNumber++;
                }
                else
                {
                    Console.WriteLine(" ");
                    break;
                }
            }

            return allRecordIds;
        }
    }

    static async Task ProcessBatchAsync(
    string entityName, List<Guid> batchRecordIds, ClientConfig clientPair, string d365Url)
    {
        _batchesstarted++;
       
        Console.SetCursorPosition(0, _reportDictionary["ReportingLine"] + 12);

        Console.Write($"batches started:{_batchesstarted}, Batches completed: {_batchesended}, Batches failed: {_batchesfailed}");
        ServiceClient conn;

        lock (connections)
        {
            if (!connections.TryGetValue(clientPair.ClientId, out conn))
            {
                // Initialize a new connection for this ClientId
                var connStr = $"AuthType=ClientSecret;url={d365Url};ClientId={clientPair.ClientId};ClientSecret={clientPair.ClientSecret}";
                conn = new ServiceClient(connStr);
                connections[clientPair.ClientId] = conn;
            }
            if (conn == null || !conn.IsReady)
            {
                // Initialize a new connection for this ClientId
                var connStr = $"AuthType=ClientSecret;url={d365Url};ClientId={clientPair.ClientId};ClientSecret={clientPair.ClientSecret}";
                conn = new ServiceClient(connStr);
                connections[clientPair.ClientId] = conn;
            }
        }

        try
        {
            if (conn != null && conn.IsReady)
            {
                // Wrap the asynchronous code in a Task.Run to maintain the async nature
                await Task.Run(async () =>
                {
                    var requests = new List<OrganizationRequest>();

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
                        Requests = new OrganizationRequestCollection(),
                        Settings = new ExecuteMultipleSettings
                        {
                            ContinueOnError = true,
                            ReturnResponses = true
                        }
                    };

                    multipleRequest.Requests.AddRange(requests);

                    var multipleResponse = (ExecuteMultipleResponse)await conn.ExecuteAsync(multipleRequest); // Use await here


                    _recorddeletedcount += batchRecordIds.Count;
                    double percentComplete = (double)_recorddeletedcount / _totalRecords * 100;
                    _batchruns++;
                    TimeSpan elapsedTime = DateTime.Now.Subtract(_startTime);
                    double timePerPercent = elapsedTime.TotalMilliseconds / percentComplete;
                    double remainingPercent = 100 - percentComplete;

                    string formattedTime = $"{elapsedTime.Hours:D2}:{elapsedTime.Minutes:D2}:{elapsedTime.Seconds:D2}";


                    int cursorPosition = _reportDictionary["ReportingLine"];
                    Console.SetCursorPosition(0, cursorPosition);

                    if (percentComplete > 0.001)
                    {
                        TimeSpan estimatedTimeRemaining = TimeSpan.FromMilliseconds(timePerPercent * remainingPercent);
                        DateTime estimatedCompletionTime = DateTime.Now.Add(estimatedTimeRemaining);
                        int processingRate = Convert.ToInt32((_recorddeletedcount / elapsedTime.TotalSeconds));
                        Console.WriteLine(
                            $"      ------------------ RUN Number: {_processingRetry}/{_allretries}".PadRight(79, '-'));
                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine($"    | Number of Records Processed: {_recorddeletedcount} of {_totalRecords}"
                            .PadRight(80) + '|');
                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine($"    | Percentage Complete          {percentComplete:F2}%".PadRight(80) + '|');
                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine($"    | Remaining Records:           {_totalRecords - _recorddeletedcount}"
                            .PadRight(80) + '|');
                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine($"    | Elapsed Time:                {elapsedTime}".PadRight(80) + '|');
                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine($"    | Remaining Time:              {estimatedTimeRemaining}".PadRight(80) +
                                          '|');
                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine(
                            $"    | Transfer rate:               {processingRate} records per second".PadRight(80) + '|');
                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine(
                            $"    | Estimated Completion time:   {estimatedCompletionTime}".PadRight(80) + '|');

                        //cursorPosition++;
                        //Console.SetCursorPosition(0, cursorPosition);
                        //Console.WriteLine("      ----------------------------".PadRight(79, '-'));

                        //cursorPosition++;
                        //Console.SetCursorPosition(0, cursorPosition);
                        //Console.WriteLine(
                        //    $"    | Batches Started:   {_batchesstarted}".PadRight(80) + '|'); cursorPosition++;
                        //cursorPosition++;
                        //Console.SetCursorPosition(0, cursorPosition);
                        //Console.WriteLine(
                        //    $"    | Batches complete/failed:   {_batchesended}/{_batchesended}".PadRight(80) + '|'); cursorPosition++;
                        //cursorPosition++;
                        //Console.SetCursorPosition(0, cursorPosition);
                        //Console.WriteLine(
                        //    $"    | Progressing Batches:   {Math_batchesstarted-_batchesended-_batchesfailed}".PadRight(80) + '|'); cursorPosition++;



                        cursorPosition++;
                        Console.SetCursorPosition(0, cursorPosition);
                        Console.WriteLine("      ----------------------------".PadRight(79, '-'));
                    }

                    //go to reporting line

                    //   Console.Write("\r-Processed {0} records. Estimated completion time: {1}. Total time spent {2} ({3} RPS)", 
                    //    _recorddeletedcount, estimatedCompletionTime, formattedTime, processingRate)
                    _batchesended++;
                }).ConfigureAwait(false); // ConfigureAwait to prevent deadlocks
                
            }
            else
            {
                _batchesfailed++;
                LogError("Batch Creation Error", conn != null ? conn.LastError : "Connection not ready");
            }
        }
        catch (Exception ex)
        {
            _batchesfailed++;
            LogError("Conn Error: ", ex.Message);
        }
        
    }


    class ClientConfig
    {
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
    }
    static void LogError(string category, string message)
    {
        lock (fileLock)
        {
            using (StreamWriter writer = File.AppendText("error.log"))
            {
                writer.WriteLine($"{category}: {message}");
            }
        }
    }
}
