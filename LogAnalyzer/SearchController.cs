﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Permissions;
using System.Threading;
using System.Threading.Tasks;
using Nest;
using Nest.Resolvers.Converters;
using Nest.Resolvers.Converters.Aggregations;
using Newtonsoft.Json;

namespace ElasticSearchConsoleApplication
{
    public enum LogFileType
    {
        ZvmLog,
        ZvmPerformanceLog
    }
    public class SearchController
    {
        private readonly ElasticClient _Client;

        public SearchController()
        {
            var node = new Uri("http://localhost:9200");

            var settings = new ConnectionSettings(
                node,
                "my-application"
            );

            _Client = new ElasticClient(settings);
        }

        public int LoadToMemory(LogFileType logFileType)
        {
            var logLoader = GetLogLoader(logFileType);

            if (!logLoader.IsAny())
            {
                Console.WriteLine("No {0} log files found in {1}", logFileType, Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs"));
                return 0;
            }

            int filesNumber = logLoader.GetFilesNumber();

            var asyncResults = new ConcurrentBag<Task<IBulkResponse>>();

            var cpuCounter = new PerformanceCounter("Processor Information", "% Processor Time", "_Total");

            var finishedResetEvent = new ManualResetEvent(false);

            var getResultsTask = new Task(() =>
            {
                while (!finishedResetEvent.WaitOne(0, false) || !asyncResults.IsEmpty)
                {
                    Task<IBulkResponse> bulkResponse;
                    if (!asyncResults.TryTake(out bulkResponse))
                    {
                        Console.WriteLine("Skipping");
                        continue;
                    }

                    if (!bulkResponse.IsCompleted)
                    {
                        bulkResponse.Wait();
                    }
                    if (bulkResponse.IsFaulted)
                    {
                        Console.WriteLine("Got error on loading:{0}", bulkResponse.Exception);
                    }
                    
                }
            });

            getResultsTask.Start();

            foreach (var logEntry in logLoader.Load(filesNumber))
            {
                IEnumerable<LogEntry> entry = logEntry;
                var result = _Client.BulkAsync(x => x.IndexMany(entry));
                asyncResults.Add(result);
            }

            finishedResetEvent.Set();





            //var processorUsage = cpuCounter.NextValue();
            //if (processorUsage > 90)
            //{
            //    Console.WriteLine(processorUsage);
            //    Thread.Sleep(TimeSpan.FromMilliseconds(400));
            //}



            return filesNumber;
        }

        private ILogLoader GetLogLoader(LogFileType logFileType)
        {
            switch (logFileType)
            {
                case LogFileType.ZvmLog:
                    return new ZvmLogLoader();
                    break;
                case LogFileType.ZvmPerformanceLog:
                    return new ZvmPerformanceLogLoader();
                    break;
                default:
                    throw new ArgumentOutOfRangeException("logFileType");
            }
        }




        public void Clean()
        {
            _Client.DeleteIndex(x => x.AllIndices());
        }


        public List<string> GetAllTaskIds()
        {
            var searchResults3 = _Client.Search<LogEntry>(s => s
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
            var searchResults3 = _Client.Search<LogEntry>(s => s
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
                    _Client.Search<LogEntry>(s => s.From(0).Size(Int32.MaxValue).Query(q => q.Term(p => p.TaskId, taskId)));

                var searchResults2 = _Client.Search<LogEntry>(s => s.From(0)
                    .Size(1)
                    .Query(q => q
                        .Term(p => p.MethodName, "settaskname") && q.Term(p => p.TaskId, taskId)));

                var searchResults3 = _Client.Search<LogEntry>(s => s.From(0)
                    .Size(1)
                    .Query(q => q
                        .Term(p => p.MethodName, "createremotemanagertask") && q.Term(p => p.TaskId, taskId)));

                var searchResults4 = _Client.Search<LogEntry>(s => s.From(0)
                    .Size(Int32.MaxValue)
                    .Query(q => q
                        .Term(p => p.LogLevel, "Error") && q.Term(p => p.TaskId, taskId)));

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
            var searchResults = _Client.Search<LogEntry>(s => s
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

            var searchResults = _Client.Search<LogEntry>(s => s.From(0)
                .Size(5000)
                .SortAscending(x => x.TimeStamp)
                .Query(q => q
                    .Term(p => p.TaskId, taskId)));


            return searchResults.Documents.ToList();
        }

        public List<LogEntry> GetTaskLogEntries(string taskId, DateTime startRange, DateTime endRange)
        {

            var searchResults = _Client.Search<LogEntry>(s => s.From(0)
                .Size(Int32.MaxValue)
                .Filter(u => u.Range(r => r.OnField(e => e.TimeStamp).GreaterOrEquals(startRange).LowerOrEquals(endRange)))
                .SortAscending(x => x.TimeStamp)
                .Query(q => q
                    .Term(p => p.TaskId, taskId)));


            return searchResults.Documents.ToList();
        }

        public List<LogEntry> GetThreadLogEntries(string threadId)
        {
            var searchResults = _Client.Search<LogEntry>(s => s.From(0)
                .Query(q => q
                    .Term(p => p.ThreadId, threadId))
                .Size(5000)
                .SortAscending(x => x.TimeStamp));



            return searchResults.Documents.ToList();
        }

        public List<LogEntry> GetThreadLogEntries(string threadId, DateTime startRange, DateTime endRange)
        {
            var searchResults = _Client.Search<LogEntry>(s => s
                .From(0)
                .Filter(u => u.Range(r => r.OnField(e => e.TimeStamp).GreaterOrEquals(startRange).LowerOrEquals(endRange)))
                .Query(q => q.Term(p => p.ThreadId, threadId))
                .Size(Int32.MaxValue)
                .SortAscending(x => x.TimeStamp));



            return searchResults.Documents.ToList();
        }

        public void FindHits(string fieldName, string termToSearch)
        {
            var properties = typeof(LogEntry).GetProperties();

            fieldName = (from a in properties
                         where a.Name.Equals(fieldName)
                         select a.Name).FirstOrDefault();

            //if (string.IsNullOrEmpty(fieldName))
            //{
            //    throw  new Exception(string.Format("Invalid fieldName {0}. It doesn't beloq to logEntry class", fieldName));
            //}

            fieldName = "TaskId";
            termToSearch = "5";

            var searchResultsDistinctMethodNames = _Client.Search<LogEntry>(s => s.From(0)
                .Aggregations(a => a.Terms("MethodNames", b => b.Field(e => e.MethodName).MinimumDocumentCount(2000).Size(50000)))
                );

            var methodNames = new List<string>();

            foreach (var s in searchResultsDistinctMethodNames.Aggregations.Values)
            {
                var bucket = s as Bucket;
                if (bucket != null)
                {
                    var items = from a in bucket.Items
                                where ((KeyItem)a).DocCount > 2000
                                orderby ((KeyItem)a).DocCount descending
                                select (KeyItem)a;

                    foreach (var item in items.Take(10))
                    {
                        Console.WriteLine("MethodName:{0} DocCount:{1}", item.Key, item.DocCount);
                        methodNames.Add(item.Key);
                    }
                }
            }

            foreach (var methodName in methodNames.Take(5))
            {
                var searchResultsMethods = _Client.Search<LogEntry>(s => s.From(0)
                    .Query(q => q.Term(t => t.OnField(n => n.MethodName).Value(methodName)))
                    .Size(100000)
                    );


                Console.WriteLine();

                foreach (var logEntry in searchResultsMethods.Documents.OrderByDescending(x => x.Message.Length).Take(3))
                {
                    Console.WriteLine(logEntry.ToStringWithoutFieldNames());
                    Console.WriteLine();
                }
            }


            //var searchResults = _Client.Search<LogEntry>(s => s.From(0)
            //    .Query(q => q
            //        .Fuzzy(x => x.OnField(f => f.TaskId).Value(termToSearch).Fuzziness(50)))
            //    //Boost(1.0).Fuzziness(1).MaxExpansions(100).PrefixLength(0))).Explain()
            //    .Size(5000)
            //    //.SortAscending(x => x.TimeStamp));
            //    );

            //foreach (var hit in searchResults.Hits.Where(x => x.Score > 1))
            //{
            //    Console.WriteLine("Score:{0}", hit.Score);
            //    Console.WriteLine(hit.Id);
            //    //Console.WriteLine("Source:{0}", hit.Source.ToStringWithoutFieldNames());
            //}
        }
    }
}