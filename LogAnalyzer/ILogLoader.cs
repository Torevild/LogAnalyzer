using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace LogAnalyzer
{
    public interface ILogLoader
    {
        int GetFilesNumber();
        bool IsAny();
        BlockingCollection<LogEntry> BeginLoad(IEnumerable<string> filenames);
        IEnumerable<string> GetFilenames(int filesNumber);
        
    }
}
