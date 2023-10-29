using Microsoft.Xrm.Tooling.Connector;
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

class Program
{
    private static int _recorddeletedcount = 0;
    private static int _totalRecords = 0;
    private static DateTime _startTime;
    private static int _batchruns  = 0;
    private static int _processingRetry,_allretries = 0;
    private static Dictionary<string, int> _reportDictionary = new Dictionary<string, int>();
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
        _allretries = Convert.ToInt32(ConfigurationManager.AppSettings.Get("ProcessingRetries"));
        _processingRetry = 1;
          
        while (_processingRetry <= _allretries)
        {
            await doProcessingAsync(entityName, batchSize, clientPairs, d365Url);
            _processingRetry++;
        }
    }

    static async Task doProcessingAsync(string entityName, int batchSize, List<ClientConfig> clientPairs, string d365Url)
    {
        Console.WriteLine("Getting All Records");

        // Fetch record IDs from Dataverse
        var recordIds = FetchAllRecordIds(entityName, clientPairs.First(), d365Url); // Example using the first client

        _totalRecords = recordIds.Count;

        // Create a dictionary to manage semaphores for each client ID
        var semaphores = new Dictionary<string, SemaphoreSlim>();

        foreach (var clientPair in clientPairs)
        {
            semaphores[clientPair.ClientId] = new SemaphoreSlim(int.Parse(ConfigurationManager.AppSettings.Get("ThreadCount")), int.Parse(ConfigurationManager.AppSettings.Get("ThreadCount"))); // Set the limit to 52 for each client
        }

        var deleteTasks = new List<Task>();
        Console.WriteLine($"Start deletion of {_totalRecords} records.");
        Console.WriteLine("");
        Console.WriteLine("");
        _startTime = DateTime.Now;

        _reportDictionary["ReportingLine"] = Console.CursorTop + 1;
        Parallel.ForEach(clientPairs, clientPair =>
        {
            var recordIdsCopy = new Queue<Guid>(recordIds);
            var batchSizeCopy = batchSize;
            var task = Task.Run(async () =>
            {
                await semaphores[clientPair.ClientId].WaitAsync(); // Acquire a semaphore slot for the specific client
                try
                {
                    while (recordIdsCopy.Count > 0)
                    {
                        var batchRecordIds = recordIdsCopy.Take(batchSizeCopy).ToList();
                        recordIdsCopy = new Queue<Guid>(recordIdsCopy.Skip(batchSizeCopy));

                        ProcessBatchAsync(entityName, batchRecordIds, clientPair, d365Url);
                    }
                }
                finally
                {
                    semaphores[clientPair.ClientId].Release(); // Release the semaphore slot for the specific client
                }
            });

            if (task != null)
            {
                deleteTasks.Add(task);
            }
        });

        // Wait for all delete tasks to complete
        await Task.WhenAll(deleteTasks.Where(t => t != null));

        // Await user input before exiting
        Console.WriteLine("Press Enter to exit.");
        Console.ReadLine();
    }

    static List<Guid> FetchAllRecordIds(string entityName, ClientConfig client, string d365Url)
    {
        var connStr = $"AuthType=ClientSecret;url={d365Url};ClientId={client.ClientId};ClientSecret={client.ClientSecret}";
        using (var conn = new CrmServiceClient(connStr))
        {
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

    static async Task ProcessBatchAsync(string entityName, List<Guid> batchRecordIds, ClientConfig clientPair, string d365Url)
    {
        var connStr = $"AuthType=ClientSecret;url={d365Url};ClientId={clientPair.ClientId};ClientSecret={clientPair.ClientSecret}";
        var conn = new CrmServiceClient(connStr);
        conn.BypassPluginExecution = true;

        try
        {
            if (conn.IsReady)
            {
                var requests = new List<OrganizationRequest>();

                foreach (var recordId in batchRecordIds)
                {
                    var deleteRequest = new DeleteRequest
                    {
                        Target = new EntityReference(entityName, recordId)
                    };

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

                var multipleResponse = (ExecuteMultipleResponse)conn.Execute(multipleRequest);

                _recorddeletedcount += batchRecordIds.Count;
                double percentComplete = (double)_recorddeletedcount / _totalRecords * 100;
                _batchruns++;
                TimeSpan elapsedTime = DateTime.Now.Subtract(_startTime);
                double timePerPercent = elapsedTime.TotalMilliseconds / percentComplete;
                double remainingPercent = 100 - percentComplete;
                
                string formattedTime = $"{elapsedTime.Hours:D2}:{elapsedTime.Minutes:D2}:{elapsedTime.Seconds:D2}";
                //go to reporting line
                
                int cursorPosition = _reportDictionary["ReportingLine"];
                Console.SetCursorPosition(0, cursorPosition);

                if (percentComplete > 0.001)
                {
                    TimeSpan estimatedTimeRemaining = TimeSpan.FromMilliseconds(timePerPercent * remainingPercent);
                    DateTime estimatedCompletionTime = DateTime.Now.Add(estimatedTimeRemaining);
                    int processingRate = Convert.ToInt32((_recorddeletedcount / elapsedTime.TotalSeconds));
                    Console.WriteLine($"      ------------------ RUN Number: {_processingRetry}/{_allretries}".PadRight(79,'-'));
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Number of Records Processed: {_recorddeletedcount} of {_totalRecords}".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Percentage Complete          {percentComplete:F2}%".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Remaining Records:           {_totalRecords - _recorddeletedcount}".PadRight(80) + '|');
                    cursorPosition++;
                    Console.SetCursorPosition(0, cursorPosition);
                    Console.WriteLine($"    | Elapsed Time:                {elapsedTime}".PadRight(80) + '|');
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
                    
                    //   Console.Write("\r-Processed {0} records. Estimated completion time: {1}. Total time spent {2} ({3} RPS)", 
                    //    _recorddeletedcount, estimatedCompletionTime, formattedTime, processingRate);
                }
                
            }
            else
            {
                // Handle the case where the connection is not ready
            }
        }
        catch (Exception ex)
        {
            // Handle the error as needed.
        }
        finally
        {
            if (conn != null)
            {
                conn.Dispose();
            }
        }
    }

    class ClientConfig
    {
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
    }
}
