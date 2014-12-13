using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ElasticSearchConsoleApplication
{
    public interface ILogLoader
    {
        int GetFilesNumber();
        IEnumerable<IEnumerable<LogEntry>> Load(int filesNumber);
        bool IsAny();
    }
}
