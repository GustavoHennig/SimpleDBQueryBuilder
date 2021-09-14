using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace GHSoftware.SimpleDb
{
    public class DbRequest
    {
        //public long RequestSize = 0;
        public string Sql { get; private set; }
        public List<KeyValuePair<string, object>> Parameters { get; private set; }
        public DbResult DbResult { get; private set; }
        public CmdType Type { get; private set; }

        public TaskCompletionSource<DbResult> TaskCompletionSource { get; set; }
        //public DbRequest(CmdType cmdType, string sql, List<KeyValuePair<string, object>> parameters = null)
        //{
        //    this.sql = sql;
        //    this.parameters = parameters;
        //    this.cmdType = cmdType;
        //}
        private DbRequest()
        {
        }

        public static DbRequest Of(CmdType cmdType, string sql, string[] columnsNames = null, List<KeyValuePair<string, object>> parameters = null)
        {
            var dbr = new DbRequest
            {
                Type = cmdType,
                Sql = sql,
                Parameters = parameters
            };

            if (columnsNames == null)
            {
                dbr.DbResult = new DbResult(1);
            }
            else
            {
                dbr.DbResult = new DbResult(columnsNames);
            }

            return dbr;
        }

        public DbRequest AddRange(IEnumerable<KeyValuePair<string, object>> parameters)
        {
            if(Parameters ==null) Parameters= new List<KeyValuePair<string, object>>();
            Parameters.AddRange(parameters);
            return this;
        }

        internal void AddRange(List<(string key, object value, string op)> parameters)
        {
           if( Parameters == null) Parameters = new List<KeyValuePair<string, object>>();
            foreach (var (key, value, _) in parameters)
            {
                Parameters.Add(new KeyValuePair<string, object>(key, value));
            }
        }

        public DbRequest AddParam(string name, object value)
        {
            if (Parameters == null) Parameters = new List<KeyValuePair<string, object>>();
            Parameters.Add(new KeyValuePair<string, object>(name, value));
            return this;
        }

        public DbRequest MergeWith(DbRequest source)
        {
            if (string.IsNullOrEmpty(Sql))
                Sql = source.Sql;
            else
                Sql += "; " + source.Sql;
            return this;
        }
        public Task<DbResult> RunAsyncWith(DbQueue dbQueue)
        {
            return dbQueue.ExecuteAsync(this);
        }

        public DbResult RunWith(DbQueue dbQueue)
        {
            return dbQueue.Execute(this);
        }

        /// <summary>
        /// This is direct approach, without Producer/Consumer pattern
        /// </summary>
        /// <param name="dbConn"></param>
        /// <returns></returns>
        public DbResult RunWith(BaseDbConn dbConn)
        {
            return dbConn.ExecuteDbRequest(this);
        }
        

        public enum CmdType
        {
            Query,
            CommandWithIdentity,
            Command,
            SingleResult,
            CloseAsap
        }

      
    }
}
