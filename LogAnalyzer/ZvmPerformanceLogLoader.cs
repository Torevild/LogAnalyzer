using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ElasticSearchConsoleApplication;

namespace LogAnalyzer
{
    public class ZvmPerformanceLogLoader : ILogLoader
    {
        private const string m_folder = "logs";
        private const string m_logFilenamePattern = "perf.*";

        public IEnumerable<IEnumerable<LogEntry>> Load(int filesNumber)
        {
            Console.WriteLine("Going to load: {0} performance log files", filesNumber);
            var stopWatch = new Stopwatch();
            foreach (var filename in Directory.EnumerateFiles(m_folder, m_logFilenamePattern, SearchOption.AllDirectories))
            {
                Task processStringsTask = null;
                var result = new ConcurrentBag<LogEntry>();
                stopWatch.Restart();
                using (var sr = new StreamReader(filename))
                {
                    while (!sr.EndOfStream)
                    {
                        var allLines = sr.ReadToEndAsync().Result.Split('\n');

                        if (processStringsTask != null)
                        {
                            processStringsTask.Wait();
                        }

                        string filename1 = filename;
                        processStringsTask = Task.Factory.StartNew(() =>
                        {
                            string logFilename = Path.GetFileNameWithoutExtension(filename1);
                            Parallel.ForEach(allLines, currentLine =>
                            {
                                if (!string.IsNullOrEmpty(currentLine))
                                {
                                    int index = GetIndexOfLastDelimeterComma(currentLine);

                                    string message;
                                    if (currentLine.Length == index + 1)
                                    {
                                        message = string.Empty;
                                    }
                                    else
                                    {
                                        message = currentLine.Substring(index + 1, currentLine.Length - index - 2);
                                    }

                                    string[] line = currentLine.Substring(0, index).Split(',');
                                    var logEntry = new LogEntry()
                                    {
                                        LogFilename = logFilename,
                                        OwnerId = string.Empty,
                                        TaskId = string.Empty,
                                        TimeStamp = DateTime.Parse(line[0]), //6/12/2014 06:24:08.618
                                        LogLevel = string.Empty,
                                        ThreadId = Convert.ToInt32(line[1]),
                                        ClassName = line[2],
                                        MethodName = line[3],
                                        Message = message
                                    };
                                    result.Add(logEntry);
                                }
                            });
                        });
                    }
                    yield return result;
                    stopWatch.Stop();
                    Console.WriteLine("Loaded file:{0}. Remain files:{1} It took:{2}", Path.GetFileName(filename), filesNumber--, stopWatch.Elapsed.ToString("g"));
                }
            }
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

        public BlockingCollection<LogEntry> BeginLoad(IEnumerable<string> filenames)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> GetFilenames(int filesNumber)
        {
            throw new NotImplementedException();
        }

        public int GetFilesNumber()
        {
            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern, SearchOption.AllDirectories).Count();
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
                    if (count == 4)
                    {
                        break;
                    }
                }
                index++;
            }
            return index;
        }
    }
}
