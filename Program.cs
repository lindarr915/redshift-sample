using System;
using System.Data;
using System.Data.Odbc;

namespace redshift.amazon.com.docsamples
{
    class ConnectToClusterExample
    {

        private static async Task ReadDataFromRedshiftAsync(int count)
        {
            // TODO: Complete the ExecuteNonQueryAsync()
            // Task<bool>[] ReadDataTask = new Task<bool>[count];

            // for (int i = 0; i < count; i++)
            // {

            // }
            // await Task.WhenAll(ReadDataTask);

        }
        
        public static void Main(string[] args)
        {

            DataSet ds = new DataSet();
            DataTable dt = new DataTable();

            // Server, e.g. "examplecluster.xyz.us-west-2.redshift.amazonaws.com"
            string server = System.Environment.GetEnvironmentVariable("DB_HOST");

            // Port, e.g. "5439"
            string port = "5439";

            // MasterUserName, e.g. "masteruser".
            string masterUsername = System.Environment.GetEnvironmentVariable("DB_USER");

            // MasterUserPassword, e.g. "mypassword".
            string masterUserPassword = System.Environment.GetEnvironmentVariable("DB_PASSWORD");

            // DBName, e.g. "dev"
            string DBName = System.Environment.GetEnvironmentVariable("DB_NAME");

            // string query = "select * from information_schema.tables;";
            string query = "select 1";

            try
            {
                // Create the ODBC connection string.
                //Redshift ODBC Driver - 64 bits
                /*
                string connString = "Driver={Amazon Redshift (x64)};" +
                    String.Format("Server={0};Database={1};" +
                    "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require",
                    server, DBName, masterUsername,
                    masterUserPassword, port);
                */

                //Redshift ODBC Driver - 32 bits
                string connString = "Driver={Amazon Redshift (x64)};" +
                    String.Format("Server={0};Database={1};" +
                    "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require",
                    server, DBName, masterUsername,
                    masterUserPassword, port);

                // Make a connection using the ODBC provider.
                OdbcConnection conn = new OdbcConnection(connString);
                conn.Open();

                // Try a simple query.
                string sql = query;
                OdbcDataAdapter da = new OdbcDataAdapter(sql, conn);
                OdbcCommand dc = new OdbcCommand(sql, conn);

                int result;
                result = dc.ExecuteNonQuery();
                // da.Fill(ds);
                // dt = ds.Tables[0];
                // foreach (DataRow row in dt.Rows)
                // {
                //     Console.WriteLine(row["table_catalog"] + ", " + row["table_name"]);
                // }
                Console.WriteLine(result);
                conn.Close();
                // Console.ReadKey();

                // Thread.Sleep(100);

                // TODO: Open a text file to run the SQL query 
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
            }

        }
    }
}
