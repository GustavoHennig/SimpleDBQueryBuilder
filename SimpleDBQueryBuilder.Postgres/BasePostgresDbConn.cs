using Npgsql;
using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;

namespace GHSoftware.SimpleDb
{
    /// <summary>
    /// This is and example for Sqlite
    /// </summary>
    public class BasePostgresDbConn : BaseDbConn
    {
        string connectionString;

        public BasePostgresDbConn(string connectionString)
        {
            this.connectionString = connectionString;
            this.IdentityRetrieve = IdentityRetrieveMode.ExecuteScalar;
        }

        protected override DbConnection CreateConnection()
        {
            return new NpgsqlConnection(connectionString);
        }

        public override bool OnSqlErrorRetry(DbException ex, int attempt)
        {
            Debug.Print($"Initialize {Name} error: {ex}, retrying {attempt}");
            
            Close();
            return true;
        }

        public override void OnAfterOpenConnection(IDbCommand dbCommand)
        {

        }

        public override long LastInsertId()
        {
            return 0;
        }

        public override string QueryToRetrieveSchemaVersion()
        {
            return "";
        }

        public override string QueryToSetSchemaVersion(int value)
        {
            return $"";
        }
    }
}
