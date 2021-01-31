using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace GHSoftware.SimpleDb
{
    public abstract class BaseDbConn
    {
        private DbConnection conn;
        public bool Initialized = false;
        protected abstract DbConnection CreateConnection();
        protected object openCloseSync = new object();
        protected string DbFileName;
        public string Name;
        int operationsRunning = 0;
        protected List<string> Migrations = new List<string>();

        public Action<string> OnLog = null;
        public void Initialize(int attempts = 5, Func<DbException, bool> onSqlErrorRetry = null)

        {
            try
            {
                conn = CreateConnection();
                //TODO: Se ocorrer exceção aqui, pensar em mecanismo de auto recuperação
                Open();
                using (var com = conn.CreateCommand())
                {
                    OnAfterOpenConnection(com);
                    RunMigrations(com);
                }
            }
            catch (DbException ex)
            {
                Close();
                bool retried = false;
                if (onSqlErrorRetry != null)
                {
                    if (onSqlErrorRetry(ex))
                    {
                        if (attempts > 0)
                        {
                            Initialize(--attempts);
                            retried = true;
                        }
                    }
                }

                if (!retried)
                    throw;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                Close();
                Initialized = true;
            }
        }

        public void RecreateConnection()
        {
            try
            {
                conn.Close();
            }
            catch (Exception ex)
            {
                OnLog?.Invoke($"RecreateConnection Close {Name} {ex}");
            }
            Initialize(2);
        }

        public bool IsOpen()
        {
            lock (openCloseSync)
            {
                return conn?.State == ConnectionState.Open;
            }
        }

        //REMEMBER 
        //protected abstract bool IsOpenInternal();

        public void Open()
        {
            lock (openCloseSync)
            {
                if (!IsOpen())
                {
                    conn.Open();
                }
            }
        }

        public void Close()
        {
            lock (openCloseSync)
            {
                if (IsOpen())
                {
                    try
                    {
                        conn.Close();
                    }
                    catch (Exception ex)
                    {
                        OnLog?.Invoke($"Failed to close connection {Name} {ex}");
                    }

                }
            }
        }

        public void LockConnection()
        {
            lock (openCloseSync)
            {
                operationsRunning++;
            }
        }

        public void ReleaseConnection()
        {
            lock (openCloseSync)
            {
                operationsRunning--;
            }
        }

        public void TryClose()
        {
            bool locked = false;
            try
            {
                locked = Monitor.TryEnter(openCloseSync);
                if (locked)
                {
                    if (operationsRunning == 0)
                    {
                        Close();
                    }
                }
                else
                {
                    OnLog?.Invoke("Failed to lock to tryclose");
                }
            }
            finally
            {
                if (locked)
                    Monitor.Exit(openCloseSync);
            }
        }


        public abstract void OnAfterOpenConnection(IDbCommand dbCommand);
        public void RunMigrations(IDbCommand com)
        {


            IDbTransaction dbTransaction = null;
            try
            {
                dbTransaction = conn.BeginTransaction();

                com.CommandText = "PRAGMA user_version;";
                int userVersion = Convert.ToInt32((long)com.ExecuteScalar());

                int i;
                for (i = userVersion; i < Migrations.Count; i++)
                {
                    try
                    {
                        com.CommandText = Migrations[i];
                        com.ExecuteNonQuery();
                    }
                    catch (DbException ex)
                    {
                        if (ex.ErrorCode != 1)
                            throw;
                    }
                }

                com.CommandText = $"PRAGMA user_version = {i};";
                com.ExecuteNonQuery();

                dbTransaction.Commit();
            }
            catch (Exception)
            {
                dbTransaction?.Rollback();
            }
        }

        public DbResult ExecuteDbRequest(DbRequest dbRequest)
        {

            var dbCommand = conn.CreateCommand();

            if (dbRequest.Parameters != null)
                foreach (var kv in dbRequest.Parameters)
                {
                    var p = dbCommand.CreateParameter();
                    p.ParameterName = "@" + kv.Key;
                    if (kv.Value == null)
                        p.Value = DBNull.Value;
                    else
                        p.Value = kv.Value;
                    dbCommand.Parameters.Add(p);
                }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
            dbCommand.CommandText = dbRequest.Sql;
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

            if (dbRequest.Type == DbRequest.CmdType.Command)
            {
                dbRequest.DbResult.AddSingle(dbCommand.ExecuteNonQuery());
            }
            else if (dbRequest.Type == DbRequest.CmdType.CommandWithIdentity)
            {
                dbCommand.ExecuteNonQuery();
                dbRequest.DbResult.AddSingle(LastInsertId());
            }
            else if (dbRequest.Type == DbRequest.CmdType.Query)
            {
                dbRequest.DbResult.LoadFromDataReader(dbCommand.ExecuteReader(CommandBehavior.SequentialAccess));
            }
            else if (dbRequest.Type == DbRequest.CmdType.SingleResult)
            {
                dbRequest.DbResult.AddSingle(dbCommand.ExecuteScalar());
            }

            return dbRequest.DbResult;

        }

        public abstract long LastInsertId();

        public DbTransaction BeginTransaction()
        {
            return conn.BeginTransaction();
        }
    }
}
