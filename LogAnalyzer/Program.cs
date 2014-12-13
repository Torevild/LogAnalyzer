using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;


namespace ElasticSearchConsoleApplication
{


    internal class Program
    {
        private static void Main(string[] args)
        {
            string folderName = string.Format("Report_{0}", DateTime.Now.ToString("yyyy_MM_dd_HH_mm_ss"));
            var searchController = new SearchController();

            var stopWatch = new Stopwatch();

            var logAnalyzer = new LogAnalyzer();

            var logValuePairs = new List<KeyValuePair<LogEntry, LogEntry>>();

            List<string> taskIds = null;
            List<string> threadIds = null;

            bool stop = false;
            while (!stop)
            {
                try
                {
                    Console.Clear();
                    PrintAllActions();

                    var userInput = Console.ReadKey();
                    Console.WriteLine();

                    switch (userInput.Key)
                    {
                        case ConsoleKey.Escape:
                            {
                                stop = true;
                                Console.WriteLine("Exiting");
                                break;
                            }

                        case ConsoleKey.D1:
                            Console.WriteLine("Loading ZVM log files to memory. Please wait");
                            stopWatch.Reset();
                            stopWatch.Start();
                            int filesNumber = searchController.LoadToMemory(LogFileType.ZvmLog);
                            stopWatch.Stop();
                            Console.WriteLine("Loaded {0} log files to memory. It took:{1}", filesNumber,
                                stopWatch.Elapsed);
                            PressAnyKeyToContinue();
                            break;

                        case ConsoleKey.D2:
                            Console.WriteLine("Loading ZVM performance log files to memory. Please wait");
                            stopWatch.Reset();
                            stopWatch.Start();
                            filesNumber = searchController.LoadToMemory(LogFileType.ZvmPerformanceLog);
                            stopWatch.Stop();
                            Console.WriteLine("Loaded {0} performance log files to memory. It took:{1}", filesNumber,
                                stopWatch.Elapsed);
                            PressAnyKeyToContinue();
                            break;

                        case ConsoleKey.D3:
                            Console.WriteLine("Getting unique task IDs");
                            stopWatch.Reset();
                            stopWatch.Start();
                            taskIds = searchController.GetAllTaskIds();
                            var pathToSave = Path.Combine(folderName, "UniqueTaskIds.txt");
                            SaveResultsToFile(pathToSave, taskIds);
                            stopWatch.Stop();
                            Console.WriteLine("Saved {0} unique task IDs to the file: {1} It took:{2}", taskIds.Count,
                                pathToSave, stopWatch.Elapsed);
                            PressAnyKeyToContinue();
                            break;

                        case ConsoleKey.D4:
                            Console.WriteLine("Getting tasks metadata");
                            stopWatch.Reset();
                            stopWatch.Start();
                            if (taskIds == null)
                            {
                                taskIds = searchController.GetAllTaskIds();
                            }
                            List<ZTaskMetadata> taskMetadata = searchController.GetTasksMetadata(taskIds);
                            pathToSave = Path.Combine(folderName, "TasksMetadata.txt");
                            SaveResultsToFile(pathToSave, taskMetadata);
                            stopWatch.Stop();
                            Console.WriteLine("Saved {0} tasks metadata to file: {1} It took:{2}", taskMetadata.Count,
                                pathToSave, stopWatch.Elapsed);
                            PressAnyKeyToContinue();
                            break;

                        case ConsoleKey.D5:
                            Console.WriteLine("Please enter thread number:");
                            int threadNumber = Convert.ToInt32(Console.ReadLine());

                            Console.WriteLine("Please enter start date time for the time range");
                            var startTimePeriod = GetRangeEdge();

                            Console.WriteLine("Please enter end date time for the time range");
                            var endTimePeriod = GetRangeEdge();

                            Console.WriteLine("Getting all log entries for thread {0} in time range from:{1} to {2}",
                                threadNumber, startTimePeriod.ToString("G"), endTimePeriod.ToString("G"));
                            stopWatch.Reset();
                            stopWatch.Start();
                            var logEntries =
                                searchController.GetLogEntriesForThread(
                                    threadNumber.ToString(CultureInfo.InvariantCulture), startTimePeriod, endTimePeriod);
                            var filename = string.Format("Thread_{0}_from_{1}_to_{2}_LogEntries.txt",
                                threadNumber, startTimePeriod.ToString("yy-MM-dd-HH-mm-ss"),
                                endTimePeriod.ToString("yy-MM-dd-HH-mm-ss"));
                            pathToSave = Path.Combine(folderName, filename);
                            SaveResultsToFile(pathToSave, logEntries);
                            stopWatch.Stop();
                            Console.WriteLine();
                            Console.WriteLine("{0} log entries for thread {1} saved to file:{2} It took:{3}",
                                logEntries.Count, threadNumber, pathToSave, stopWatch.Elapsed);
                            PressAnyKeyToContinue();
                            break;

                        case ConsoleKey.D6:
                            var timeThreshold = GetTimeThreshold();

                            Console.WriteLine("Please enter start date time for the time range");
                            startTimePeriod = GetRangeEdge();

                            Console.WriteLine("Please enter end date time for the time range");
                            endTimePeriod = GetRangeEdge();

                            Console.WriteLine(
                                "Getting log entries for every task which exceeds the threshold:{0} in time range from:{1} to {2}",
                                timeThreshold, startTimePeriod.ToString("G"), endTimePeriod.ToString("G"));
                            stopWatch.Reset();
                            stopWatch.Start();

                            if (taskIds == null)
                            {
                                taskIds = searchController.GetAllTaskIds();
                            }

                            int totalSavedPairs = 0;

                            foreach (var taskId in taskIds)
                            {
                                logEntries = searchController.GetTaskLogEntries(taskId, startTimePeriod, endTimePeriod);
                                logValuePairs = logAnalyzer.GetLogEntriesWithTimeGaps(logEntries, timeThreshold);

                                var stringBuilder = new StringBuilder();
                                foreach (var logValuePair in logValuePairs)
                                {
                                    stringBuilder.AppendLine(logValuePair.Key.ToStringWithoutFieldNames());
                                    stringBuilder.AppendLine(logValuePair.Value.ToStringWithoutFieldNames());
                                    stringBuilder.AppendLine();
                                }
                                totalSavedPairs += logValuePairs.Count;

                                filename = string.Format("Task_{0}_from_{1}_to_{2}_logEntriesExceedThreshold.txt",
                                    taskId, startTimePeriod.ToString("yy-MM-dd-HH-mm-ss"),
                                    endTimePeriod.ToString("yy-MM-dd-HH-mm-ss"));
                                pathToSave = Path.Combine(folderName, filename);
                                SaveResultsToFile(pathToSave, stringBuilder.ToString());
                                Console.WriteLine();
                                Console.WriteLine(
                                    "Saved {0} log entries for task:{1} which exceeds the threshold to file:{2}",
                                    logValuePairs.Count, taskId, pathToSave);
                            }
                            stopWatch.Stop();

                            Console.WriteLine("Saved total {0} log entries which exceeds the threshold. It took:{1}",
                                totalSavedPairs, stopWatch.Elapsed);
                            PressAnyKeyToContinue();
                            break;

                        case ConsoleKey.D7:
                            {
                                timeThreshold = GetTimeThreshold();

                                Console.WriteLine("Please enter start date time for the time range");
                                startTimePeriod = GetRangeEdge();

                                Console.WriteLine("Please enter end date time for the time range");
                                endTimePeriod = GetRangeEdge();

                                Console.WriteLine(
                                    "Getting log entries for every thread which exceeds the threshold:{0} in time range from:{1} to {2}",
                                    timeThreshold, startTimePeriod.ToString("G"), endTimePeriod.ToString("G"));

                                stopWatch.Reset();
                                stopWatch.Start();

                                if (threadIds == null)
                                {
                                    threadIds = searchController.GetAllThreadIds();
                                }

                                logValuePairs = new List<KeyValuePair<LogEntry, LogEntry>>();

                                foreach (var threadId in threadIds)
                                {
                                    logEntries = searchController.GetThreadLogEntries(threadId, startTimePeriod,
                                        endTimePeriod);
                                    logValuePairs = logAnalyzer.GetLogEntriesWithTimeGaps(logEntries, timeThreshold);

                                    var stringBuilder = new StringBuilder();
                                    foreach (var logValuePair in logValuePairs)
                                    {
                                        stringBuilder.AppendLine(logValuePair.Key.ToStringWithoutFieldNames());
                                        stringBuilder.AppendLine(logValuePair.Value.ToStringWithoutFieldNames());
                                        stringBuilder.AppendLine();
                                    }
                                    filename = string.Format("Thread_{0}_from_{1}_to_{2}_logEntriesExceedThreshold.txt",
                                        threadId, startTimePeriod.ToString("yy-MM-dd-HH-mm-ss"),
                                        endTimePeriod.ToString("yy-MM-dd-HH-mm-ss"));
                                    pathToSave = Path.Combine(folderName, filename);

                                    SaveResultsToFile(pathToSave, stringBuilder.ToString());
                                    Console.WriteLine(
                                        "Saved {0} log entries for thread:{1} which exceeds the threshold to file:{2}",
                                        logValuePairs.Count, threadId, pathToSave);
                                }
                                stopWatch.Stop();
                                Console.WriteLine(
                                    "Found in total {0} log entries for {1} threads which exceeds the threshold. It took:{2}",
                                    logValuePairs.Count, threadIds.Count, stopWatch.Elapsed);
                                PressAnyKeyToContinue();
                                break;
                            }
                        case ConsoleKey.D8:
                            Console.WriteLine("Please enter field for search:");
                            string fieldName = "1";//Console.ReadLine();
                            Console.WriteLine("Please enter term for search:");
                            string termToSearch = "2";//Console.ReadLine();
                            Console.WriteLine("Searching term hits...");
                            searchController.FindHits(fieldName, termToSearch);
                            Console.WriteLine("Found");
                            PressAnyKeyToContinue();
                            break;

                        case ConsoleKey.D9:
                            Console.WriteLine("Cleaning...");
                            searchController.Clean();
                            Console.WriteLine("All cleaned");
                            PressAnyKeyToContinue();
                            break;


                        default:
                            Console.WriteLine("Invalid choice");
                            PressAnyKeyToContinue();
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Got error:{0}", ex.Message);
                    Console.WriteLine();
                    Console.WriteLine("Detailed error:{0}", ex);

                    PressAnyKeyToContinue();
                }
            }

        }

        private static DateTime GetRangeEdge()
        {
            Console.WriteLine("Please enter a year:");

            string input = Console.ReadLine();
            int year;
            if (!string.IsNullOrEmpty(input))
            {
                year = Convert.ToInt32(input);
            }
            else
            {
                year = DateTime.Now.Year;
                Console.WriteLine("Set default value:{0}", year);
            }

            Console.WriteLine("Please enter a month:");
            input = Console.ReadLine();
            int month;
            if (!string.IsNullOrEmpty(input))
            {
                month = Convert.ToInt32(input);
            }
            else
            {
                month = 1;
                Console.WriteLine("Set default value:{0}", month);
            }
            Console.WriteLine("Please enter a day:");
            input = Console.ReadLine();
            int day;
            if (!string.IsNullOrEmpty(input))
            {
                day = Convert.ToInt32(input);
            }
            else
            {
                day = 1;
                Console.WriteLine("Set default value:{0}", day);
            }
            Console.WriteLine("Please enter hours:");
            input = Console.ReadLine();
            int hours;
            if (!string.IsNullOrEmpty(input))
            {
                hours = Convert.ToInt32(input);
            }
            else
            {
                hours = 0;
                Console.WriteLine("Set default value:{0}", hours);
            }
            Console.WriteLine("Please enter minutes:");
            input = Console.ReadLine();
            int minutes;
            if (!string.IsNullOrEmpty(input))
            {
                minutes = Convert.ToInt32(input);
            }
            else
            {
                minutes = 0;
                Console.WriteLine("Set default value:{0}", minutes);
            }
            Console.WriteLine("Please enter seconds:");
            input = Console.ReadLine();
            int seconds;
            if (!string.IsNullOrEmpty(input))
            {
                seconds = Convert.ToInt32(input);
            }
            else
            {
                seconds = 0;
                Console.WriteLine("Set default value:{0}", seconds);
            }

            try
            {
                var dateTime = new DateTime(year, month, day, hours, minutes, seconds);
                return dateTime;
            }
            catch (ArgumentOutOfRangeException ex)
            {
                throw new Exception(string.Format("Invalid date time for range entered:{0}", ex.Message));
            }

        }

        private static void PressAnyKeyToContinue()
        {
            Console.WriteLine("Press any key to continue");
            Console.ReadLine();
        }

        private static TimeSpan GetTimeThreshold()
        {
            Console.WriteLine("Please set up time threshold");
            Console.WriteLine("Please enter minutes");
            var input = Console.ReadLine();
            int minutes;
            if (!string.IsNullOrEmpty(input))
            {
                minutes = Convert.ToInt32(input);
            }
            else
            {
                minutes = 0;
                Console.WriteLine("Set default value:{0}", minutes);
            }
            Console.WriteLine("Please enter seconds");
            input = Console.ReadLine();
            int seconds;
            if (!string.IsNullOrEmpty(input))
            {
                seconds = Convert.ToInt32(input);
            }
            else
            {
                seconds = 0;
                Console.WriteLine("Set default value:{0}", seconds);
            }
            Console.WriteLine("Please enter milliseconds");
            input = Console.ReadLine();
            int milliseconds;
            if (!string.IsNullOrEmpty(input))
            {
                milliseconds = Convert.ToInt32(input);
            }
            else
            {
                milliseconds = 0;
                Console.WriteLine("Set default value:{0}", milliseconds);
            }

            if ((minutes == 0 && seconds == 0 && milliseconds == 0) || minutes < 0 || seconds < 0 || milliseconds < 0)
            {
                throw new Exception("Invalid threshold time entered!");
            }

            var timeThreshold = new TimeSpan(0, 0, minutes, seconds, milliseconds);
            return timeThreshold;
        }

        private static void PrintAllActions()
        {
            Console.WriteLine("1 - Load ZVM log files to memory");
            Console.WriteLine("2 - Load ZVM performance log files to memory");
            Console.WriteLine("3 - Get all task IDs");
            Console.WriteLine("4 - Get metadata for each task");
            Console.WriteLine("5 - Get all log entries for thread");
            Console.WriteLine("6 - Get all log entries by taskId that exceed time threshold");
            Console.WriteLine("7 - Get all log entries by threadId that exceed time threshold");
            Console.WriteLine("8 - Find term hits");
            Console.WriteLine("9 - Clean all");
            Console.WriteLine("ESC - Exit");
        }

        private static void SaveResultsToFile(string fileName, IEnumerable<Object> dataToSave)
        {
            var stringBuilder = new StringBuilder();
            foreach (var o in dataToSave)
            {
                stringBuilder.AppendLine(o.ToString());
            }

            if (stringBuilder.Length == 0)
                return;

            string folderName = Path.GetDirectoryName(fileName);

            if (string.IsNullOrEmpty(folderName))
            {
                Console.WriteLine("Output folder name cannot be null or empty");
                return;
            }

            if (!Directory.Exists(folderName))
            {
                Directory.CreateDirectory(folderName);
            }

            using (var sw = new StreamWriter(fileName))
            {
                sw.WriteLine(stringBuilder.ToString());
            }
        }

        private static void SaveResultsToFile(string fileName, string dataToSave)
        {
            if (string.IsNullOrEmpty(dataToSave))
                return;

            string folderName = Path.GetDirectoryName(fileName);

            if (string.IsNullOrEmpty(folderName))
            {
                Console.WriteLine("Output folder name cannot be null or empty");
                return;
            }

            if (!Directory.Exists(folderName))
            {
                Directory.CreateDirectory(folderName);
            }

            using (var sw = new StreamWriter(fileName))
            {
                sw.WriteLine(dataToSave);
            }
        }
    }
}
