using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace LogAnalyzer
{
    public class GeneralLogLoader
    {
        protected virtual int GetIndexOfLastDelimeterComma(string line)
        {
            return 0;
        }

        protected BlockingCollection<LogEntry> BeginParse(ConcurrentQueue<KeyValuePair<string, string[]>> queue)
        {
            var results = new BlockingCollection<LogEntry>();
            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    KeyValuePair<string, string[]> item;
                    if (!queue.TryDequeue(out item))
                    {
                        Task.Delay(TimeSpan.FromMilliseconds(100));
                        continue;
                    }

                    if (item.Key == null && item.Value == null)
                    {
                        results.Add(null);
                        break;
                    }

                    //stopWatch.Restart();
                    string logFilename = Path.GetFileNameWithoutExtension(item.Key);
                    Parallel.ForEach(item.Value, currentLine =>
                    {
                        if (!string.IsNullOrEmpty(currentLine))
                        {
                            //Skipe the line with headers
                            if (currentLine.IndexOf(',') != 8)
                            {
                                return;
                            }

                            var index = GetIndexOfLastDelimeterComma(currentLine);


                            string[] line = currentLine.Substring(0, index).Split(',');
                            var logEntry = new LogEntry()
                            {
                                LogFilename = logFilename,
                                OwnerId = line[0],
                                TaskId = line[1],
                                TimeStamp =
                                    DateTime.ParseExact(line[2], "yy-MM-dd HH:mm:ss.ff",
                                        CultureInfo.CurrentCulture),
                                LogLevel = line[3],
                                ThreadId = Convert.ToInt32(line[4]),
                                ClassName = line[5],
                                MethodName = line[6],
                                Message = currentLine.Substring(index + 1, currentLine.Length - index - 2)
                            };

                            while (results.Count > 50000)
                            {
                                Task.Delay(TimeSpan.FromMilliseconds(100));
                            }

                            results.Add(logEntry);
                        }
                    });
                    Console.WriteLine("finished to parse file:{0}.", logFilename);
                }
            });
            return results;
        }

        protected ConcurrentQueue<KeyValuePair<string, string[]>> BeginReadFiles(IEnumerable<string> filenames)
        {
            var filesContentQueue = new ConcurrentQueue<KeyValuePair<string, string[]>>();

            Task.Factory.StartNew(() =>
            {
                foreach (var filename in filenames)
                {
                    using (var sr = new StreamReader(filename))
                    {
                        while (filesContentQueue.Count > 50000)
                        {
                            Task.Delay(TimeSpan.FromMilliseconds(100));
                        }
                        filesContentQueue.Enqueue(new KeyValuePair<string, string[]>(filename,
                            sr.ReadToEndAsync().Result.Split('\n')));
                        Console.WriteLine("finished to read file:{0}.", filename);
                    }
                }
                filesContentQueue.Enqueue(new KeyValuePair<string, string[]>(null, null));
            });
            return filesContentQueue;
        }
    }
}
