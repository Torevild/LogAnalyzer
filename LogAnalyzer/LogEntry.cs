using System;

namespace LogAnalyzer
{
    public class LogEntry
    {
        public string LogFilename { get; set; }
        public string OwnerId { get; set; }
        public string TaskId { get; set; }
        public DateTime TimeStamp { get; set; }
        public string LogLevel { get; set; }
        public int ThreadId { get; set; }
        public string ClassName { get; set; }
        public string MethodName { get; set; }
        public string Message { get; set; }

        public override string ToString()
        {
            return
                string.Format(
                    "LogFilename:{0} OwnerId:{1} TaskId:{2} TimeStamp:{3} LogLevel:{4} ThreadId:{5}, ClassName:{6} MethodName:{7} Message:{8}",
                    LogFilename, OwnerId, TaskId, TimeStamp.ToString("yyyy-MM-dd HH:mm:ss.FFF"), LogLevel, ThreadId, ClassName, MethodName, Message);
        }

        public string ToStringWithoutFieldNames()
        {
            return
                string.Format(
                    "{0},{1},{2},{3},{4},{5},{6},{7},{8}",
                    LogFilename, OwnerId, TaskId, TimeStamp.ToString("yyyy-MM-dd HH:mm:ss.FFF"), LogLevel, ThreadId, ClassName, MethodName, Message);
        }
    }
}
