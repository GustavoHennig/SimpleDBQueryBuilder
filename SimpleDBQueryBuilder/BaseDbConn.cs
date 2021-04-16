using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace GHSoftware.SimpleDb
{
    public abstract class BaseDbConn : IDisposable
    {
        protected DbConnection conn;
        public bool Initialized = false;
        protected abstract DbConnection CreateConnection();
        protected object openCloseSync = new object();
        /// <summary>
        /// TODO: Remove it from here, it's a Sqlite field
        /// </summary>
        protected string DbFileName;
        public string Name;
        int operationsRunning = 0;
        protected List<string> Migrations = new List<string>();

        public void Initialize(int attempts = 5)

        {
            try
            {
                conn = CreateConnection();
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

                if (OnSqlErrorRetry(ex, attempts))
                {
                    if (attempts > 0)
                    {
                        Initialize(--attempts);
                        retried = true;
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
                OnLog($"RecreateConnection Close {Name} {ex}");
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
                        OnLog($"Failed to close connection {Name} {ex}");
                    }

                }
            }
        }
        public void Dispose()
        {
            Close();
            try
            {
                conn?.Dispose();
            }
            catch (Exception)
            {
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
                    OnLog("Failed to lock to tryclose");
                }
            }
            finally
            {
                if (locked)
                    Monitor.Exit(openCloseSync);
            }
        }


        public abstract void OnAfterOpenConnection(IDbCommand dbCommand);

        /// <summary>
        /// Called after each retry when opening connection, must return true to continue retrying.
        /// >> Should this method be virtual?
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="attempt"></param>
        /// <returns></returns>
        public abstract bool OnSqlErrorRetry(DbException ex, int attempt);


        public void RunMigrations(IDbCommand com)
        {


            IDbTransaction dbTransaction = null;
            try
            {
                dbTransaction = conn.BeginTransaction();

                com.CommandText = QueryToRetrieveSchemaVersion();
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

                com.CommandText = QueryToSetSchemaVersion(i);
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
        public abstract string QueryToRetrieveSchemaVersion();
        public abstract string QueryToSetSchemaVersion(int value);
        public abstract void OnLog(string msg);

        public DbTransaction BeginTransaction()
        {
            return conn.BeginTransaction();
        }
    }
}
