using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;

namespace GHSoftware.SimpleDb
{
    public class SdbRequest

    {
        //public long RequestSize = 0;
        public string Sql { get; private set; }
        public List<KeyValuePair<string, object>> Parameters { get; private set; }
        public SdbResult DbResult { get; private set; }
        public CmdType Type { get; private set; }
        public int TimeOut { get; set; }
 
        public TaskCompletionSource<SdbResult> TaskCompletionSource { get; set; }
        //public DbRequest(CmdType cmdType, string sql, List<KeyValuePair<string, object>> parameters = null)
        //{
        //    this.sql = sql;
        //    this.parameters = parameters;
        //    this.cmdType = cmdType;
        //}
        private SdbRequest()
        {
        }

        public static SdbRequest Of(CmdType cmdType, string sql, string[] columnsNames = null, List<KeyValuePair<string, object>> parameters = null)
        {
            var dbr = new SdbRequest
            {
                Type = cmdType,
                Sql = sql,
                Parameters = parameters
            };

            if (columnsNames == null)
            {
                dbr.DbResult = new SdbResult(1);
            }
            else
            {
                dbr.DbResult = new SdbResult(columnsNames);
            }

            return dbr;
        }

        public SdbRequest Timeout(int timeOut)
        {
            TimeOut = timeOut;
            return this;
        }

        public SdbRequest AddParamRange(IEnumerable<KeyValuePair<string, object>> parameters)
        {
            if(Parameters ==null) Parameters= new List<KeyValuePair<string, object>>();
            Parameters.AddRange(parameters);
            return this;
        }

        internal void AddParamRange(List<(string key, object value)> parameters)
        {
           if( Parameters == null) Parameters = new List<KeyValuePair<string, object>>();
            foreach (var (key, value) in parameters)
            {
                Parameters.Add(new KeyValuePair<string, object>(key, value));
            }
        }

        public SdbRequest AddParam(string name, object value)
        {
            if (Parameters == null) Parameters = new List<KeyValuePair<string, object>>();
            Parameters.Add(new KeyValuePair<string, object>(name, value));
            return this;
        }

        public SdbRequest MergeWith(SdbRequest source)
        {
            if (string.IsNullOrEmpty(Sql))
                Sql = source.Sql;
            else
                Sql += "; " + source.Sql;
            return this;
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
