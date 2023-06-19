using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Diagnostics;

namespace redshift.amazon.com.docsamples
{
    class ConnectToClusterExample
    {
        static string? connString;

        private static async Task ReadDataFromRedshiftAsync(int count = 5)
        {
            // TODO: Complete the ExecuteNonQueryAsync()
            Task<int>[] ReadDataTask = new Task<int>[count];

            using (OdbcConnection connection = new OdbcConnection(connString))
            {
                connection.Open();

                // Perform your database operations here
                for (int i = 0; i < count; i++)
                {
                    string query = "select 1";

                    using (OdbcCommand command = new OdbcCommand(query, connection))
                    {
                        ReadDataTask[i] = command.ExecuteNonQueryAsync();
                    }
                }

                await Task.WhenAll(ReadDataTask);

                for (int i = 0; i < count; i++)
                {
                    Console.WriteLine(String.Format("Task {0} completed. Result is {1}.", i, ReadDataTask[i].Result.ToString()));
                }
                connection.Close();
            }



        }

        public static async Task Main(string[] args)
        {

            DataSet ds = new DataSet();
            DataTable dt = new DataTable();

            // Server, e.g. "examplecluster.xyz.us-west-2.redshift.amazonaws.com"
            string? server = System.Environment.GetEnvironmentVariable("DB_HOST");

            // Port, e.g. "5439"
            string? port = "5439";

            // MasterUserName, e.g. "masteruser".
            string? masterUsername = System.Environment.GetEnvironmentVariable("DB_USER");

            // MasterUserPassword, e.g. "mypassword".
            string? masterUserPassword = System.Environment.GetEnvironmentVariable("DB_PASSWORD");

            // DBName, e.g. "dev"
            string? DBName = System.Environment.GetEnvironmentVariable("DB_NAME");

            // string query = "select * from information_schema.tables;";
            string query = "select 1";

            try
            {
                // Create the ODBC connection string.
                //Redshift ODBC Driver - 64 bits
                connString = "Driver={Amazon Redshift (x64)};" +
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


                // TODO: Open a text file to run the SQL query 

                for (int i = 0; i < 5; i++)
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    Console.WriteLine("Iteration {0}", i);
                    await ReadDataFromRedshiftAsync(5);
                     stopwatch.Stop();
                     TimeSpan duration = stopwatch.Elapsed;
                     Console.WriteLine("Task duration: " + duration);

                    await Task.Delay(100);
                }

                // Read args[0]
                string fileName = args[0];

                string filePath = "file.sql";

                try
                {
                    // Read the entire file content as a string
                    string fileContent = File.ReadAllText(filePath);
                    Console.WriteLine(fileContent);

                    // Alternatively, read the file content line by line
                    // using a StreamReader
                    // using (StreamReader reader = new StreamReader(filePath))
                    // {
                    //     string line;
                    //     while ((line = reader.ReadLine()) != null)
                    //     {
                    //         Console.WriteLine(line);
                    //     }
                    // }
                }
                catch (IOException e)
                {
                    Console.WriteLine("An error occurred while reading the file: " + e.Message);
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
            }

        }
    }
}
