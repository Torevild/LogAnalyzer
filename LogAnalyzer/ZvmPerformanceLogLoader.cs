using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace LogAnalyzer
{
    public class ZvmPerformanceLogLoader : GeneralLogLoader, ILogLoader
    {
        private const string c_folder = "logs";
        private const string c_logFilenamePattern = "perf.*";

        public BlockingCollection<LogEntry> BeginLoad(IEnumerable<string> filenames)
        {
            var filesContentsQueue = BeginReadFiles(filenames);

            var logEntriesQueue = BeginParse(filesContentsQueue);

            return logEntriesQueue;
        }

        public IEnumerable<string> GetFilenames(int filesNumber)
        {
            return Directory.EnumerateFiles(c_folder, c_logFilenamePattern, SearchOption.AllDirectories);
        }

        public bool IsAny()
        {
            if (!Directory.Exists(c_folder))
            {
                Console.WriteLine("Path {0} doesn't exist", Path.Combine(AppDomain.CurrentDomain.BaseDirectory, c_folder));
                return false;
            }
            return Directory.EnumerateFiles(c_folder, c_logFilenamePattern, SearchOption.AllDirectories).Any();
        }

       

        public int GetFilesNumber()
        {
            return Directory.EnumerateFiles(c_folder, c_logFilenamePattern, SearchOption.AllDirectories).Count();
        }

        protected override int GetIndexOfLastDelimeterComma(string line)
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
