using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSearchConsoleApplication
{
    public class ZvmLogLoader:ILogLoader
    {
        private const string m_folder = "logs";
        private const string m_logFilenamePattern = "log*.csv";

        public IEnumerable<IEnumerable<LogEntry>> Load(int filesNumber)
        {
            Console.WriteLine("Going to load: {0} zvm log files", filesNumber);
            var stopWatch = new Stopwatch();
            foreach (var filename in Directory.EnumerateFiles(m_folder, m_logFilenamePattern, SearchOption.AllDirectories))
            {
                Task processStringsTask = null;
                var list = new ConcurrentBag<LogEntry>();
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
                                    list.Add(logEntry);
                                }
                            });
                        });
                    }
                    yield return list;
                    stopWatch.Stop();
                    Console.WriteLine("Loaded file:{0}. Remain files:{1} It took:{2}", Path.GetFileName(filename), filesNumber--, stopWatch.Elapsed.ToString("g"));
                }
            }
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
