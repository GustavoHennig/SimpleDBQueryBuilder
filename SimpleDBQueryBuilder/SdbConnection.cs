using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace GHSoftware.SimpleDb
{
    public class SdbConnection : IDisposable
    {
        public bool Initialized { get; private set; } = false;
        public SdbSetup dbConfig = null;

        protected DbConnection conn;

        protected object openCloseSync = new object();



        public DbConnection Connection
        {
            get
            {
                return conn;
            }
        }

        private int operationsRunning = 0;


        public SdbConnection(SdbSetup setup)
        {
            this.dbConfig = setup;
        }



        /// <summary>
        /// Run migrations and OnAfterOpenConnection
        /// </summary>
        /// <param name="attempts"></param>
        public void InitializeFull(int attempts = 5)

        {
            try
            {
                conn = dbConfig.CreateConnection();
                Open();
                dbConfig.OnAfterOpenConnection(conn);
                if (dbConfig.MigrationConfig?.Migrations != null)
                {
                    SdbMigrationRunner.RunMigrations(conn, dbConfig.MigrationConfig);
                }
            }
            catch (DbException ex)
            {
                Close();
                bool retried = false;

                if (dbConfig.OnSqlErrorRetry(this, ex, attempts))
                {
                    if (attempts > 0)
                    {
                        InitializeFull(--attempts);
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

        /// <summary>
        /// Just create the connection, still closed
        /// </summary>
        public void InitializeSlim()
        {
            conn = dbConfig.CreateConnection();
        }


        public void RecreateConnection()
        {
            try
            {
                conn.Close();
            }
            catch (Exception ex)
            {
                dbConfig.OnLog($"RecreateConnection Close {dbConfig.Name} {ex}");
            }
            InitializeFull(2);
        }

        public bool IsOpen()
        {
            lock (openCloseSync)
            {
                return conn?.State == ConnectionState.Open;
            }
        }

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
                        dbConfig.OnLog($"Failed to close connection {dbConfig.Name} {ex}");
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
                    dbConfig.OnLog("Failed to lock to tryclose");
                }
            }
            finally
            {
                if (locked)
                    Monitor.Exit(openCloseSync);
            }
        }

        public DbTransaction BeginTransaction()
        {
            return conn.BeginTransaction();
        }

    }
}
