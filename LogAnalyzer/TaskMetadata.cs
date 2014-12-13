using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ElasticSearchConsoleApplication
{
    public class ZTaskMetadata
    {
        public string TaskId { get; set; }
        public DateTime StarTime { get; set; }
        public DateTime StopTime { get; set; }

        public string Name { get; set; }

        public bool HasErrors { get; set; }

        public override string ToString()
        {
            return
                string.Format(
                    "TaskId:{0} StartTime:{1} StopTime:{2} Name:{3} HasErrors:{4}",
                    TaskId, StarTime.ToString("yyyy-MM-dd HH:mm:ss.FFF"), StopTime.ToString("yyyy-MM-dd HH:mm:ss.FFF"), Name, HasErrors);
        }
    }
}
