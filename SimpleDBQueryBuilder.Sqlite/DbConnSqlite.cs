using System;
using System.Data;
using System.Data.SQLite;

namespace GHSoftware.SimpleDb
{
    /// <summary>
    /// TODO
    /// </summary>
    public abstract class DbConnSqlite : BaseDbConn
    {
        protected SQLiteConnection conn;

        public override long LastInsertId()
        {
            throw new NotImplementedException();
        }

        public override void OnAfterOpenConnection(IDbCommand dbCommand)
        {
            throw new NotImplementedException();
        }
    }
}
