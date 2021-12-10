using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using static GHSoftware.SimpleDb.SdbSetup;

namespace GHSoftware.SimpleDb
{
    public static class SdbRequestRunner

    {
        public static SdbResult ExecuteDbRequest(this DbConnection conn, SdbRequest dbRequest, SdbSetup dbConf)
        {
            using (var dbCommand = conn.CreateCommand())
            {
                if (dbRequest.TimeOut > 0)
                {
                    dbCommand.CommandTimeout = dbRequest.TimeOut;
                }

                if (dbRequest.Parameters != null)
                    foreach (var kv in dbRequest.Parameters)
                    {
                        var p = dbCommand.CreateParameter();
                        p.ParameterName = "@" + kv.Key;
                        if (kv.Value == null)
                        {
                            p.Value = DBNull.Value;
                        }
                        else if (kv.Value is Enum)
                        {
                            p.Value = (int)kv.Value;
                        }
                        else
                        {
                            p.Value = kv.Value;
                        }
                        dbCommand.Parameters.Add(p);
                    }

                dbCommand.CommandText = dbRequest.Sql;

                if (dbRequest.Type == SdbRequest.CmdType.Command)
                {
                    dbRequest.DbResult.AddSingle(dbCommand.ExecuteNonQuery());
                }
                else if (dbRequest.Type == SdbRequest.CmdType.CommandWithIdentity)
                {
                    if (dbConf.IdentityRetrieve == IdentityRetrieveMode.ExecuteScalar)
                    {
                        dbRequest.DbResult.AddSingle(dbCommand.ExecuteScalar());
                    }
                    else
                    {
                        dbCommand.ExecuteNonQuery();

                        
                        
                            dbRequest.DbResult.AddSingle(dbConf.LastInsertId(conn));
                        
                    }
                }
                else if (dbRequest.Type == SdbRequest.CmdType.Query)
                {
                    dbRequest.DbResult.LoadFromDataReader(dbCommand.ExecuteReader(CommandBehavior.SequentialAccess));
                }
                else if (dbRequest.Type == SdbRequest.CmdType.SingleResult)
                {
                    dbRequest.DbResult.AddSingle(dbCommand.ExecuteScalar());
                }
            }

            return dbRequest.DbResult;
        }
    }
}
