using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using static GHSoftware.SimpleDb.SdbSetup;

namespace GHSoftware.SimpleDb
{
    /// <summary>
    /// This is and example for Postgres
    /// </summary>
    public class SdbSetupPostgresDefault : SdbSetup
    {
        private readonly string connectionString;

        public SdbSetupPostgresDefault(string connectionString)
        {
            this.connectionString = connectionString;
            this.IdentityRetrieve = IdentityRetrieveMode.ExecuteScalar;
        }

        public override DbConnection CreateConnection()
        {
            return new NpgsqlConnection(connectionString);
        }

        public override void OnLog(string msg)
        {
        }

        public override long LastInsertId(DbConnection dbConnection)
        {
            return 0;

        }

        public override void OnAfterOpenConnection(DbConnection dbConnection)
        {

        }

        public override bool OnSqlErrorRetry(SdbConnection conn, DbException ex, int attempt)
        {
            Debug.Print($"Initialize {Name} error: {ex}, retrying {attempt}");

            conn.Close();
            return true;
        }
    }
}
