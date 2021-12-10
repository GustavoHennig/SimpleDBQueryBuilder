using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace GHSoftware.SimpleDb
{
    public static class SdbRequestExtensionsPostgres
    {

        static private SdbSetupPostgresDefault postgresDefault = new SdbSetupPostgresDefault("");
        /// <summary>
        /// This is direct approach, without Producer/Consumer pattern
        /// </summary>
        /// <param name="dbConn"></param>
        /// <returns></returns>
        public static SdbResult RunWith(this SdbRequest dbRequest, DbConnection dbConn)
        {
            return SdbRequestRunner.ExecuteDbRequest(dbConn, dbRequest, postgresDefault);
        }
    }
}
