using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Configuration;
using System.IO;
using System.Data;
using System.Globalization;
using System.Data.SqlClient;

namespace PerfMon.lib
{
    static class PerfCounters
    {
        internal static bool ValidateStorage()
        {
            try
            {
                Console.WriteLine("Validating Storage Account \n");
                CloudStorageAccount slabLogsStorageAccount =
                CloudStorageAccount.Parse(ConfigurationManager.AppSettings["DiagnosticsStorageAccount"]);
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Validation of storage account failed... \n check app.config for DiagnosticsStorageAccount key");
                return false;
            }
        }

        internal static bool ValidateTestDetails(string runid)
        {
            if (DbHelper.isValidTest(int.Parse(runid)))
                return true;
            else
                return false;
        }

        internal static async void InitiateDownload(string runId, List<DateTime> times)
        {
            Console.WriteLine($"Beginning Download of counters for runid {runId}");
            TableQuery<DynamicTableEntity> query = new TableQuery<DynamicTableEntity>()
             .Where(
                 TableQuery.CombineFilters(
                     TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual,
                         times[0].Ticks.ToString("d19", (IFormatProvider)CultureInfo.InvariantCulture))
                     ,
                     TableOperators.And,
                     TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual,
                         times[1].Ticks.ToString("d19", (IFormatProvider)CultureInfo.InvariantCulture))
                 ));
            if (runId == "CustomImport")
                await BeginDownload(query, runId, false).ConfigureAwait(false);
            else
            {
                await BeginDownload(query, runId, true).ConfigureAwait(false);
                await DbHelper.LoadPerfCounterData(runId);
            }

        }

        private static async Task<bool> BeginDownload(TableQuery<DynamicTableEntity> query, string runid,bool isTable)
        {
            DataTable perfdataTable = new DataTable();
            SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(ConfigurationManager.AppSettings["LoadTestDBConString"]) ;
            StringBuilder sb = new StringBuilder();

            if (isTable)
            {
                perfdataTable = DbHelper.InitializeDataTable();
                sqlBulkCopy = DbHelper.InitializeCopyMap(ConfigurationManager.AppSettings["LoadTestDBConString"]);
            }
            else
            {
                sb.AppendLine("PreciseTime,MachineName,CounterCategory,CounterInstance,CounterName,CounterValue");
            }
            CloudStorageAccount counterStorageAccount =
               CloudStorageAccount.Parse(ConfigurationManager.AppSettings["DiagnosticsStorageAccount"]);
            CloudTableClient perfCounterTableClient = counterStorageAccount.CreateCloudTableClient();
            CloudTable perfCounterTable = perfCounterTableClient.GetTableReference("WADPerformanceCountersTable");

            TableContinuationToken token = null;
            var linecount = 0;
            int categoryMarker = 0, instanceMarker = 0;
            string category = string.Empty, instance = string.Empty, countername = string.Empty;
            do
            {
                TableQuerySegment<DynamicTableEntity> segment = perfCounterTable.ExecuteQuerySegmented(query, token);
                token = segment.ContinuationToken;
                if (segment.Results.Count != 0)
                {
                    foreach (DynamicTableEntity counter in segment)
                    {
                        DataRow row = perfdataTable.NewRow();
                        string temp = counter["CounterName"].StringValue;
                        try
                        {
                            if (temp.IndexOf(")") != -1)
                            {
                                categoryMarker = temp.IndexOf("(");
                                category = temp.Substring(1, categoryMarker - 1);
                                temp = temp.Substring(categoryMarker + 1);
                                instanceMarker = temp.IndexOf(")");
                                instance = temp.Substring(0, (instanceMarker));
                                countername = temp.Substring(instanceMarker + 2);
                            }
                            else
                            {
                                categoryMarker = temp.IndexOf(@"\", 2);
                                category = temp.Substring(1, (categoryMarker - 1));
                                instance = "systemdiagnosticsperfcounterlibsingleinstance";
                                countername = temp.Substring(categoryMarker + 1);
                            }
                            if (isTable)
                            {
                                row["RunId"] = runid;
                                row["CounterCollectionTime"] = counter.Properties["PreciseTimeStamp"].DateTime;
                                row["MachineName"] = counter.Properties["RoleInstance"].StringValue;
                                row["CounterCategory"] = category;
                                row["CounterInstance"] = instance;
                                row["CounterName"] = countername;
                                row["CounterValue"] = counter.Properties["CounterValue"].DoubleValue;
                                perfdataTable.Rows.Add(row);
                            }
                            else
                            {
                    sb.AppendLine(counter["PreciseTimeStamp"].DateTime + "," +
                    counter["RoleInstance"].StringValue + "," + category + "," + instance + "," + countername + "," +
                    counter["CounterValue"].DoubleValue);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception : " + e.Message);
                        }
                        ++linecount;
                    }
                    if (linecount >0)
                    {
                        try
                        {
                            if (isTable)
                            {
                                sqlBulkCopy.WriteToServer(perfdataTable);
                                perfdataTable.Rows.Clear();
                                linecount = 0;
                            }
                            else
                            {
                                var filename = string.Concat("PerfCounters", Guid.NewGuid().ToString().Replace('-', '_') + ".csv");
                                using (StreamWriter sw = File.CreateText(Path.Combine(ConfigurationManager.AppSettings["CSVFileLocation"], filename)))
                                {
                                    sw.Write(sb.ToString());
                                    sb.Clear();
                                    sb.AppendLine("PreciseTime,MachineName,CounterCategory,CounterInstance,CounterName,CounterValue");
                                    linecount = 0;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception when writing data " + e.Message);
                        }
                    }
                }
            } while (token != null);
            if (linecount > 0)
            {
                if (isTable)
                {
                    sqlBulkCopy.WriteToServer(perfdataTable);
                    perfdataTable.Rows.Clear();
                    linecount = 0;
                }
                else
                {
                    var filename = string.Concat("PerfCounters", Guid.NewGuid().ToString().Replace('-', '_') + ".csv");
                    using (StreamWriter sw = File.CreateText(Path.Combine(ConfigurationManager.AppSettings["CSVFileLocation"], filename)))
                    {
                        sw.Write(sb.ToString());
                        sb.Clear();
                        sb.AppendLine("PreciseTime,MachineName,CounterCategory,CounterInstance,CounterName,CounterValue");
                        linecount = 0;
                    }
                }
            }
            return true;
        }
    }
}
