using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PerfMon.lib;
using System.Globalization;

namespace PerfMon
{
    class PerfMon
    {
        static void Main(string[] args)
        {
            string runId = string.Empty;
            string startTime = string.Empty;
            string endTime = string.Empty;
            List<DateTime> times = new List<DateTime>();

            if (args.Length == 1)
            {
                foreach (string arg in args)
                {
                    string tArg = arg.Trim();
                    try
                    {
                        if (tArg.Substring(0, 7).ToLower() == "-runid:")
                        {
                            runId = tArg.Substring(7).Trim();
                            if (DbHelper.ValidateConnection() && PerfCounters.ValidateStorage())
                            {
                                if (PerfCounters.ValidateTestDetails(runId))
                                {
                                    PerfCounters.InitiateDownload(runId, DbHelper.GetTestTimes(runId));
                                }
                            }
                        }
                        else if (tArg.Substring(0, 6).ToLower() == "-usage")
                        {
                            Console.WriteLine("RunId of load test is mandatory if start and end time is not provided");
                            Console.WriteLine("Usage PerfMon -rundid:<valid run id> \n " +
                                "or \n" +
                                "PerfMon -st:<Start time to begin collecting perfmon data> -ed:<end time>");
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Usage PerfMon -rundid:<valid run id> \n " +
                        "or \n" +
                        "PerfMon -st:<Start time to begin collecting perfmon data> -ed:<end time>");
                    }
                }
            }
            else if (args.Length == 2)
            {
                try
                {
                    foreach (string arg in args)
                    {
                        string tArg = arg.Trim();
                        if (tArg.Substring(0, 4).ToLower() == "-st:")
                        {
                            startTime = tArg.Substring(4).Trim();
                            times.Add(DateTime.ParseExact(startTime, "MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal));
                        }
                        else if (tArg.Substring(0, 4).ToLower() == "-ed:")
                        {
                            endTime = tArg.Substring(4).ToLower().Trim();
                            times.Add(DateTime.ParseExact(endTime, "MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal));
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Usage PerfMon -rundid:<valid run id> \n " +
                    "or \n" +
                    "PerfMon -st:<Start time to begin collecting perfmon data> -ed:<end time>");
                }

                if (DbHelper.ValidateConnection() && PerfCounters.ValidateStorage())
                {
                    PerfCounters.InitiateDownload("CustomImport", times);
                }
            }
            else
            {
                Console.WriteLine("Usage PerfMon -rundid:<valid run id> \n " +
                "or \n" +
                "PerfMon -st:<Start time to begin collecting perfmon data> -ed:<end time>");
            }
        }
    }
}
