using System;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;

namespace GHSoftware.SimpleDb
{
    /// <summary>
    /// This is and example for Sqlite
    /// </summary>
    public abstract class BaseSqliteDbConn : BaseDbConn
    {
        protected override DbConnection CreateConnection()
        {
            string strConn = "DATA SOURCE=" + DbFileName;
            return new SQLiteConnection(strConn);
        }

        public override bool OnSqlErrorRetry(DbException ex, int attempt)
        {
            Debug.Print($"Initialize {Name} error: {ex}, retrying {attempt}");

            Close();
            switch ((ex as SQLiteException).ResultCode)
            {
                case SQLiteErrorCode.NotADb:
                    File.Delete(DbFileName + ".bad");
                    File.Move(DbFileName, DbFileName + ".bad");
                    break;
                case SQLiteErrorCode.Misuse:
                    // Didn't understand this error, it occurs in VM, just retry and it works.
                    break;
                case SQLiteErrorCode.Corrupt:
                    if (attempt == 1)
                    {
                        Open();
                        using (var com = conn.CreateCommand())
                        {
                            com.CommandText = "VACUUM;";
                            com.ExecuteNonQuery();
                        }
                    }
                    break;
            }
            return true;
        }

        public override void OnAfterOpenConnection(IDbCommand dbCommand)
        {
            dbCommand.CommandText = "PRAGMA journal_mode=WAL;";
            dbCommand.ExecuteNonQuery();

            dbCommand.CommandText = "PRAGMA synchronous = FULL;";
            dbCommand.ExecuteNonQuery();

            dbCommand.CommandText = "PRAGMA auto_vacuum = 1;";
            dbCommand.ExecuteNonQuery();
        }

        public override long LastInsertId()
        {
            return ((SQLiteConnection)conn).LastInsertRowId;
        }

        public override string QueryToRetrieveSchemaVersion()
        {
            return "PRAGMA user_version;";
        }

        public override string QueryToSetSchemaVersion(int value)
        {
            return $"PRAGMA user_version = {value};";
        }
    }
}
