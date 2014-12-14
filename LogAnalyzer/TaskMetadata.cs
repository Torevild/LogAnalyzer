using System;

namespace LogAnalyzer
{
    public class ZTaskMetadata
    {
        public string TaskId { get; set; }
        public DateTime StarTime { get; set; }
        public DateTime StopTime { get; set; }

        public string Name { get; set; }

        public bool HasErrors { get; set; }

        public LogEntry ErrorLogEntry { get; set; }

        public override string ToString()
        {
            return
                string.Format(
                    "TaskId:{0} StartTime:{1} StopTime:{2} Name:{3} HasErrors:{4}. \n\t{5}",
                    TaskId, StarTime.ToString("yyyy-MM-dd HH:mm:ss.FFF"), StopTime.ToString("yyyy-MM-dd HH:mm:ss.FFF"), Name, HasErrors, HasErrors ? ErrorLogEntry.ToStringWithoutFieldNames() : string.Empty);
        }
    }
}
