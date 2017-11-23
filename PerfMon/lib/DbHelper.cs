using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.IO;

namespace PerfMon.lib
{
    public static class DbHelper
    {
        static SqlConnection connection = new SqlConnection();
        public static bool ValidateConnection()
        {
            connection.ConnectionString = ConfigurationManager.AppSettings["LoadTestDBConString"];
            try
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandText = @"if OBJECT_ID('" + ConfigurationManager.AppSettings["DBSchemaName"] + "." +
                    ConfigurationManager.AppSettings["PerfCounterTempTableName"] + @"','U') is null BEGIN CREATE TABLE " + ConfigurationManager.AppSettings["DBSchemaName"] + "."
                    + ConfigurationManager.AppSettings["PerfCounterTempTableName"] + @"(
                [RunId][nvarchar](50) NOT NULL,
                [CounterCollectionTime] [datetime] NOT NULL,
                [MachineName] [nvarchar] (500) NOT NULL,
                [CounterCategory] [nvarchar] (500) NOT NULL,
                [CounterInstance] [nvarchar] (500) NOT NULL,
                [CounterName] [nvarchar] (500) NOT NULL,
                [CounterValue] [float] NOT NULL
                ) ON[PRIMARY] END";
                cmd.Connection = connection;
          cmd.ExecuteScalar();
                 return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error validating connection to Loadtest database... \n " +
                    "validate connection details");
                return false;
            }
        }

        internal static List<DateTime> GetTestTimes(string testid)
        {
            List<DateTime> times = new List<DateTime>();
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select starttime,endtime from dbo.loadtestrun where LoadTestRunId =@testid";
            cmd.Parameters.AddWithValue("@testid", testid);
            if (connection.State == ConnectionState.Open)
            {
                try
                {
                    SqlDataReader myreader = cmd.ExecuteReader();
                    while (myreader.Read())
                    {
                        times.Add(myreader.GetDateTime(0));
                        times.Add(myreader.GetDateTime(1));
                    }
                    myreader.Close();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error retrieving test times - check the test id \n");
                }
            }
            return times;
        }

        internal static Task<int> LoadPerfCounterData(string runId)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.CommandType = CommandType.Text;
            string s = File.ReadAllText(@"..\..\lib\sqlquery.txt");
            cmd.CommandText = s;
            cmd.Parameters.AddWithValue("@loadtestrunid", runId);
            if (connection.State == ConnectionState.Open)
            {
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error loading performance counter data  into database \n " + e.Message);
                }
            }
            return Task.FromResult(1);
        }

        internal static bool isValidTest(int testid)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = "select starttime,endtime from loadtestrun where LoadTestRunId =@testid";
            cmd.Parameters.AddWithValue("testid", testid);
            if (connection.State == ConnectionState.Open)
            {
                try
                {
                    SqlDataReader myreader = cmd.ExecuteReader(CommandBehavior.SingleRow);
                    if (myreader.FieldCount > 0)
                    {
                        myreader.Close();
                        return true;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error retrieving test times - check the test id \n");
                    return false;
                }
            }
            return false;
        }

        internal static DataTable InitializeDataTable()
        {
            #region initializeDataTable

            DataTable perfdataTable = new DataTable(ConfigurationManager.AppSettings["PerfCounterTempTableName"]);
            perfdataTable.Columns.Add("RunId", typeof(string));
            perfdataTable.Columns.Add("CounterCollectionTime", typeof(DateTime));
            perfdataTable.Columns.Add("MachineName", typeof(string));
            perfdataTable.Columns.Add("CounterCategory", typeof(string));
            perfdataTable.Columns.Add("CounterInstance", typeof(string));
            perfdataTable.Columns.Add("CounterName", typeof(string));
            perfdataTable.Columns.Add("CounterValue", typeof(float));
            #endregion
            return perfdataTable;
        }
        internal static SqlBulkCopy InitializeCopyMap(string v)
        {
            SqlBulkCopy bulkCopy = new SqlBulkCopy(v);
            bulkCopy.ColumnMappings.Add("RunId", "RunId");
            bulkCopy.ColumnMappings.Add("CounterCollectionTime", "CounterCollectionTime");
            bulkCopy.ColumnMappings.Add("MachineName", "MachineName");
            bulkCopy.ColumnMappings.Add("CounterCategory", "CounterCategory");
            bulkCopy.ColumnMappings.Add("CounterInstance", "CounterInstance");
            bulkCopy.ColumnMappings.Add("CounterName", "CounterName");
            bulkCopy.ColumnMappings.Add("CounterValue", "CounterValue");
            bulkCopy.BatchSize = 10000;
            bulkCopy.BulkCopyTimeout = 0;
            bulkCopy.DestinationTableName = ConfigurationManager.AppSettings["PerfCounterTempTableName"];
            return bulkCopy;
        }
    }
}
