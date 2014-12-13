using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace ElasticSearchConsoleApplication
{
    public class ZvmPerformanceLogLoader:ILogLoader
    {
        private const string m_folder = "logs";
        private const string m_logFilenamePattern = "perf.*";

        public IEnumerable<IEnumerable<LogEntry>> Load(int filesNumber)
        {
            Console.WriteLine("Going to load: {0} performance log files", filesNumber);
            var stopWatch = new Stopwatch();
            foreach (var filename in Directory.EnumerateFiles(m_folder, m_logFilenamePattern))
            {
                var result = new List<LogEntry>();
                stopWatch.Restart();
                using (var sr = new StreamReader(filename))
                {
                    while (!sr.EndOfStream)
                    {
                        var readLine = sr.ReadLine();

                        if (!string.IsNullOrEmpty(readLine))
                        {
                            int count = 0;
                            int index = 0;
                            foreach (char c in readLine)
                            {
                                if (c == ',')
                                {
                                    count++;
                                    if (count == 4)
                                    {
                                        break;
                                    }
                                }
                                index++;
                            }

                            string message;
                            if (readLine.Length == index + 1)
                            {
                                message = string.Empty;
                            }
                            else
                            {
                                message = readLine.Substring(index + 1, readLine.Length - index - 2);
                            }

                            string[] line = readLine.Substring(0, index).Split(',');
                            var logEntry = new LogEntry()
                            {
                                LogFilename = Path.GetFileNameWithoutExtension(filename),
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
                    }
                    yield return result;
                    stopWatch.Stop();
                    Console.WriteLine("Loaded file:{0}. Remain files:{1} It took:{2}", Path.GetFileName(filename), filesNumber--, stopWatch.Elapsed.ToString("g"));
                }
            }
        }

        public bool IsAny()
        {
            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern).Any();
        }

        public int GetFilesNumber()
        {
            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern).Count();
        }
    }
}
