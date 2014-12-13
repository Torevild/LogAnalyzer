using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LogAnalyzer
{
    public class ZvmLogLoader : ILogLoader
    {
        private const string m_folder = "logs";
        private const string m_logFilenamePattern = "log*.csv";

        public BlockingCollection<LogEntry> BeginLoad(IEnumerable<string> filenames)
        {

            var filesContentsQueue = BeginReadFiles(filenames);

            var logEntriesQueue = BeginParse(filesContentsQueue);

            return logEntriesQueue;
        }

        private BlockingCollection<LogEntry> BeginParse(ConcurrentQueue<KeyValuePair<string, string[]>> queue)
        {
            var results = new BlockingCollection<LogEntry>();
            Task.Factory.StartNew(() =>
            {
                //var stopWatch = new Stopwatch();

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
                    //stopWatch.Stop();
                    //Console.WriteLine("finished to parse file:{0}. It took:{1}", logFilename, stopWatch.Elapsed);
                }
            });
            return results;
        }

        private ConcurrentQueue<KeyValuePair<string, string[]>> BeginReadFiles(IEnumerable<string> filenames)
        {
            var filesContentQueue = new ConcurrentQueue<KeyValuePair<string, string[]>>();

            Task.Factory.StartNew(() =>
            {
                foreach (var filename in filenames)
                {
                    //var stopWatch = new Stopwatch();
                    using (var sr = new StreamReader(filename))
                    {
                        while (filesContentQueue.Count > 50000)
                        {
                            Task.Delay(TimeSpan.FromMilliseconds(100));
                        }
                        //stopWatch.Restart();
                        filesContentQueue.Enqueue(new KeyValuePair<string, string[]>(filename,
                            sr.ReadToEndAsync().Result.Split('\n')));
                        //stopWatch.Stop();
                        //Console.WriteLine("finished to read file:{0}. It took:{1}", filename, stopWatch.Elapsed);
                    }
                }
                filesContentQueue.Enqueue(new KeyValuePair<string, string[]>(null, null));
            });
            return filesContentQueue;
        }

        public IEnumerable<string> GetFilenames(int filesNumber)
        {
            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern, SearchOption.AllDirectories);
        }

        private static int GetIndexOfLastDelimeterComma(string line)
        {
            int count = 0;
            int index = 0;
            for (int i = 0; i < line.Length; i++)
            {
                if (line[i] == ',')
                {
                    count++;
                    if (count == 7)
                    {
                        break;
                    }
                }
                index++;
            }
            return index;
        }

        public IEnumerable<IEnumerable<LogEntry>> Load(int filesNumber)
        {
            throw new NotImplementedException();
        }

        public bool IsAny()
        {
            if (!Directory.Exists(m_folder))
            {
                Console.WriteLine("Path {0} doesn't exist", Path.Combine(AppDomain.CurrentDomain.BaseDirectory, m_folder));
                return false;
            }

            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern, SearchOption.AllDirectories).Any();
        }

        public int GetFilesNumber()
        {
            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern, SearchOption.AllDirectories).Count();
        }
    }


}
