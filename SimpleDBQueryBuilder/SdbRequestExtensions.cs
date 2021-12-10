using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace GHSoftware.SimpleDb
{
    public static class SdbRequestExtensions
    {
        public static Task<SdbResult> RunAsyncWith(this SdbRequest SdbRequest, SdbQueue dbQueue)
        {
            return dbQueue.ExecuteAsync(SdbRequest);
        }

        public static SdbResult RunWith(this SdbRequest SdbRequest, SdbQueue dbQueue)
        {
            return dbQueue.Execute(SdbRequest);
        }

        /// <summary>
        /// This is direct approach, without Producer/Consumer pattern
        /// </summary>
        /// <param name="dbConn"></param>
        /// <returns></returns>
        public static SdbResult RunWith(this SdbRequest SdbRequest, SdbConnection dbConn)
        {
            return SdbRequestRunner.ExecuteDbRequest(dbConn.Connection, SdbRequest, dbConn.dbConfig);
        }


    }
}
