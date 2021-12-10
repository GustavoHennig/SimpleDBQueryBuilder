using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace GHSoftware.SimpleDb
{
    public abstract class SdbSetup
    {
        public string Name { get; set; }

        public IdentityRetrieveMode IdentityRetrieve = IdentityRetrieveMode.LastInsertId;
        public SdbMigrationConfig MigrationConfig { get; set; }
        public abstract long LastInsertId(DbConnection dbConnection);

        /// <summary>
        /// Called after Full Initialize
        /// </summary>
        /// <param name="dbCommand"></param>
        public abstract void OnAfterOpenConnection(DbConnection dbConnection);

        /// <summary>
        /// Should return a closed connection
        /// </summary>
        /// <returns></returns>
        public abstract DbConnection CreateConnection();


        /// <summary>
        /// Called after each retry when opening connection, must return true to continue retrying.
        /// >> Should this method be virtual?
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="attempt"></param>
        /// <returns></returns>
        public abstract bool OnSqlErrorRetry(SdbConnection conn, DbException ex, int attempt);

        public abstract void OnLog(string msg);

        public enum IdentityRetrieveMode
        {
            LastInsertId,
            ExecuteScalar
        }
    }



}
