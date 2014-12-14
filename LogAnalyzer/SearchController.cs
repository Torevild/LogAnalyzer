using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ElasticSearchConsoleApplication;
using Nest;

namespace LogAnalyzer
{
    public enum LogFileType
    {
        ZvmLog,
        ZvmPerformanceLog
    }
    public class SearchController
    {
        private readonly ElasticSearchProxy _ElasticSearchProxy;

        public SearchController()
        {
            _ElasticSearchProxy = new ElasticSearchProxy(null);
        }

        public int LoadToMemory(LogFileType logFileType)
        {
            var logLoader = GetLogLoader(logFileType);

            if (!logLoader.IsAny())
            {
                Console.WriteLine("No {0} log files found in {1}", logFileType, Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs"));
                return 0;
            }

            int totalFiles = logLoader.GetFilesNumber();
            int filesNumber = totalFiles;

            Console.WriteLine("Going to load: {0} zvm log files", filesNumber);

            var queue = logLoader.BeginLoad(logLoader.GetFilenames(filesNumber));



            var task = Task.Factory.StartNew(() =>
            {
                var list = new List<LogEntry>();

                var cancellationTokenSource = new CancellationTokenSource();
                CancellationToken cancellationToken = cancellationTokenSource.Token;

                while (true)
                {
                    try
                    {
                        var client = _ElasticSearchProxy.GetConnection();
                        foreach (var logEntry in queue.GetConsumingEnumerable(cancellationToken))
                        {
                            if (logEntry == null)
                            {
                                cancellationTokenSource.Cancel();
                            }

                            if (cancellationToken.IsCancellationRequested)
                            {
                                if (list.Count > 0)
                                {
                                    var internalList = Interlocked.Exchange(ref list, new List<LogEntry>());
                                    client.BulkAsync(x => x.IndexMany(internalList));
                                    list.Clear();
                                }
                                return;
                            }

                            list.Add(logEntry);

                            if (list.Count > 20000)
                            {
                                var internalList = Interlocked.Exchange(ref list, new List<LogEntry>());
                                Task.Factory.StartNew(() =>
                                {
                                    client.BulkAsync(x => x.IndexMany(internalList));
                                });
                            }

                        }
                    }
                    catch (AggregateException ex)
                    {
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("mu");
                    }
                }

            });

            task.Wait();


            return filesNumber;
        }

        private ILogLoader GetLogLoader(LogFileType logFileType)
        {
            switch (logFileType)
            {
                case LogFileType.ZvmLog:
                    return new ZvmLogLoader();
                case LogFileType.ZvmPerformanceLog:
                    return new ZvmPerformanceLogLoader();
                default:
                    throw new ArgumentOutOfRangeException("logFileType");
            }
        }




        public void Clean()
        {
            _ElasticSearchProxy.GetConnection().DeleteIndexAsync(x => x.AllIndices());
        }


        public List<string> GetAllTaskIds()
        {
            var searchResults3 = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s
                .From(0)
              .Filter(x => x.Bool(y => y.Must(z => z.Term("TaskId", "00000000"))))
              .Aggregations(x => x.Terms("LogEntry", y => y.Field("taskId")))
              .Size(1000)
              );

            var taskIds = new List<string>();

            foreach (var s in searchResults3.Aggregations.Values)
            {
                var bucket = s as Bucket;
                if (bucket == null)
                    continue;

                foreach (var aggregation in bucket.Items)
                {
                    var item = (KeyItem)aggregation;
                    if (item.Key.Contains("00000000"))
                        continue;

                    taskIds.Add(item.Key);
                }
            }
            return taskIds;
        }

        public List<string> GetAllThreadIds()
        {
            var searchResults3 = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s
                .From(0)
                .Aggregations(x => x.Terms("LogEntry", y => y.Field(t => t.ThreadId).Size(500)))
              );

            var threadIds = new List<string>();

            foreach (var s in searchResults3.Aggregations.Values)
            {
                var bucket = s as Bucket;
                if (bucket != null)
                {
                    threadIds.AddRange(from KeyItem item in bucket.Items select item.Key);
                }
            }
            return threadIds;
        }

        public List<ZTaskMetadata> GetTasksMetadata(List<string> taskIds)
        {
            var taskMetadatas = new List<ZTaskMetadata>();

            foreach (var taskId in taskIds)
            {
                var searchResults =
                    _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0).Size(Int32.MaxValue).Query(q => q.Term(p => p.TaskId, taskId)));

                var searchResults2 = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                    .Size(1)
                    .Query(q => q
                        .Term(p => p.MethodName, "settaskname") && q.Term(p => p.TaskId, taskId)));

                var searchResults3 = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                    .Size(1)
                    .Query(q => q
                        .Term(p => p.MethodName, "createremotemanagertask") && q.Term(p => p.TaskId, taskId)));

                var searchResults4 = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                    .Size(1)
                    .Query(q => q.Term(t1=>t1.OnField(x=>x.LogLevel).Value("error")) && q.Term(t2=>t2.OnField(f=>f.TaskId).Value(taskId)))
                );

                var searchDocuments = searchResults.Documents.ToList();
                bool hasErrors = searchResults4.Documents.Any();

                string name = string.Empty;
                var taskNameEntry = searchResults2.Documents.FirstOrDefault();
                if (taskNameEntry != null)
                {
                    name = taskNameEntry.Message;
                }
                else
                {
                    taskNameEntry = searchResults3.Documents.FirstOrDefault();
                    if (taskNameEntry != null)
                    {
                        name = taskNameEntry.Message;
                    }
                }

                taskMetadatas.Add(new ZTaskMetadata()
                {
                    TaskId = searchDocuments.First().TaskId,
                    StarTime = searchDocuments.First().TimeStamp,
                    StopTime = searchDocuments.Last().TimeStamp,
                    Name = name,
                    HasErrors = hasErrors
                });
            }
            return taskMetadatas;
        }



        public List<LogEntry> GetLogEntriesForThread(string threadId, DateTime startRange, DateTime endRange)
        {
            var searchResults = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s
                .From(0)
                .Filter(u => u.Range(r => r.OnField(e => e.TimeStamp).GreaterOrEquals(startRange).LowerOrEquals(endRange)))
                .Size(Int32.MaxValue)
                .SortAscending(x => x.TimeStamp)
                .Query(q => q
                    .Term(p => p.ThreadId, threadId)));


            var result = searchResults.Documents.ToList();
            if (result.Count == Int32.MaxValue)
            {
                Console.WriteLine("Possible not all results were shown due to max limitation");
            }
            return result;
        }

        public List<LogEntry> GetTaskLogEntries(string taskId)
        {

            var searchResults = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                .Size(5000)
                .SortAscending(x => x.TimeStamp)
                .Query(q => q
                    .Term(p => p.TaskId, taskId)));


            return searchResults.Documents.ToList();
        }

        public List<LogEntry> GetTaskLogEntries(string taskId, DateTime startRange, DateTime endRange)
        {

            var searchResults = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                .Size(Int32.MaxValue)
                .Filter(u => u.Range(r => r.OnField(e => e.TimeStamp).GreaterOrEquals(startRange).LowerOrEquals(endRange)))
                .SortAscending(x => x.TimeStamp)
                .Query(q => q
                    .Term(p => p.TaskId, taskId)));


            return searchResults.Documents.ToList();
        }

        public List<LogEntry> GetThreadLogEntries(string threadId)
        {
            var searchResults = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                .Query(q => q
                    .Term(p => p.ThreadId, threadId))
                .Size(5000)
                .SortAscending(x => x.TimeStamp));



            return searchResults.Documents.ToList();
        }

        public List<LogEntry> GetThreadLogEntries(string threadId, DateTime startRange, DateTime endRange)
        {
            var searchResults = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s
                .From(0)
                .Filter(u => u.Range(r => r.OnField(e => e.TimeStamp).GreaterOrEquals(startRange).LowerOrEquals(endRange)))
                .Query(q => q.Term(p => p.ThreadId, threadId))
                .Size(Int32.MaxValue)
                .SortAscending(x => x.TimeStamp));



            return searchResults.Documents.ToList();
        }

        public List<string> GetUniqueMethodNames(int maxNumber, int minHitsCount)
        {
            var searchResultsDistinctMethodNames = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                .Aggregations(a => a.Terms("MethodNames", b => b.Field(e => e.MethodName).MinimumDocumentCount(minHitsCount).Size(maxNumber)))
                );

            var uniqueMethodNames = new List<string>();

            foreach (var s in searchResultsDistinctMethodNames.Aggregations.Values)
            {
                var bucket = s as Bucket;
                if (bucket != null)
                {
                    var items = from a in bucket.Items
                                orderby ((KeyItem)a).DocCount descending
                                select (KeyItem)a;

                    foreach (var item in items)
                    {
                        uniqueMethodNames.Add(item.Key);
                    }
                }
            }
            return uniqueMethodNames;
        }

        public List<LogEntry> GetHighRateLogLinesOrderedByLength(List<string> uniqueMethodNames)
        {
            //var properties = typeof(LogEntry).GetProperties();

            //fieldName = (from a in properties
            //             where a.Name.Equals(fieldName)
            //             select a.Name).FirstOrDefault();

            //if (string.IsNullOrEmpty(fieldName))
            //{
            //    throw  new Exception(string.Format("Invalid fieldName {0}. It doesn't beloq to logEntry class", fieldName));
            //}

            //fieldName = "TaskId";
            //termToSearch = "5";

            var uniqueLogEntriesStringBuilder = new StringBuilder();
            var list = new List<LogEntry>();

            foreach (var methodName in uniqueMethodNames)
            {
                var searchResultsMethods = _ElasticSearchProxy.GetConnection().Search<LogEntry>(s => s.From(0)
                    .Query(q => q.Term(t => t.OnField(n => n.MethodName).Value(methodName)))
                    .Size(40)
                    );

                HashSet<int> uniqueMessages = new HashSet<int>();

                foreach (var logEntry in searchResultsMethods.Documents.OrderByDescending(x => x.Message.Length))
                {
                    if (!uniqueMessages.Contains(logEntry.Message.Length))
                    {
                        list.Add(logEntry);
                        uniqueMessages.Add(logEntry.Message.Length);
                    }
                }
            }
            return list;
        }
    }
}
