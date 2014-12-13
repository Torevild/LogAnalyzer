using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;

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
            foreach (var filename in Directory.EnumerateFiles(m_folder, m_logFilenamePattern))
            {
                var list = new ConcurrentBag<LogEntry>();
                stopWatch.Restart();
                using (var sr = new StreamReader(filename))
                {
                    while (!sr.EndOfStream)
                    {
                        var readLine = sr.ReadLine();

                        if (readLine != null)
                        {
                            if (readLine.IndexOf(',') != 8)
                                continue;

                            var index = GetIndexOfLastDelimeterComma(readLine);

                            string[] line = readLine.Substring(0, index).Split(',');
                            var logEntry = new LogEntry()
                           {
                               LogFilename = Path.GetFileNameWithoutExtension(filename),
                               OwnerId = line[0],
                               TaskId = line[1],
                               TimeStamp = DateTime.ParseExact(line[2], "yy-MM-dd HH:mm:ss.ff", CultureInfo.CurrentCulture),
                               LogLevel = line[3],
                               ThreadId = Convert.ToInt32(line[4]),
                               ClassName = line[5],
                               MethodName = line[6],
                               Message = readLine.Substring(index + 1, readLine.Length - index - 2)
                           };
                            list.Add(logEntry);
                        }
                    }
                    yield return list;
                    stopWatch.Stop();
                    Console.WriteLine("Loaded file:{0}. Remain files:{1} It took:{2}", Path.GetFileName(filename), filesNumber--, stopWatch.Elapsed.ToString("g"));
                }
            }
        }

        private static int GetIndexOfLastDelimeterComma(string readLine)
        {
            int count = 0;
            int index = 0;
            foreach (char c in readLine)
            {
                if (c == ',')
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
            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern).Any();
        }

        public int GetFilesNumber()
        {
            return Directory.EnumerateFiles(m_folder, m_logFilenamePattern).Count();
        }
    }

  
}
