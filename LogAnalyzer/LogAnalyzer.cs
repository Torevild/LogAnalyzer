using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LogAnalyzer
{
    public class LogAnalyzer
    {
        public List<KeyValuePair<LogEntry, LogEntry>> GetLogEntriesWithTimeGaps(List<LogEntry> logEntries, TimeSpan threshold)
        {
            var results = new List<KeyValuePair<LogEntry, LogEntry>>();

            int coresNumber = Environment.ProcessorCount;

            int logEntriesForEachTask = logEntries.Count/coresNumber;

            var tasks = new List<Task<List<KeyValuePair<LogEntry, LogEntry>>>>();

            for (int i = 0; i < coresNumber - 1; i++)
            {
                int i1 = i;
                tasks.Add(Task.Factory.StartNew(() => ProcessLogEntries(logEntries.Skip(i1 * logEntriesForEachTask).Take(logEntriesForEachTask).ToList(), threshold)));
            }
            tasks.Add(Task.Factory.StartNew(() => ProcessLogEntries(logEntries.Skip((coresNumber - 1) * logEntriesForEachTask).ToList(), threshold)));

            foreach (var task in tasks)
            {
                task.Wait();
                results.AddRange(task.Result);
            }
            
            
            return results;
        }

        private static List<KeyValuePair<LogEntry, LogEntry>> ProcessLogEntries(List<LogEntry> logEntries, TimeSpan threshold)
        {
            var results = new List<KeyValuePair<LogEntry, LogEntry>>();

            for (int i = 1; i < logEntries.Count; i++)
            {
                if ((logEntries[i].TimeStamp - logEntries[i - 1].TimeStamp) > threshold)
                {
                    results.Add(new KeyValuePair<LogEntry, LogEntry>(logEntries[i - 1], logEntries[i]));
                }
            }
            return results;
        }
    }
}
