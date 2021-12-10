using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace GHSoftware.SimpleDb
{
    /// <summary>
    /// Expermiental RequestsQueue
    /// 
    /// WARNING: This is not trustable, you may lose data when an error occurs
    /// 
    /// TODO: Handle SQLITE_BUSY (não ocorre, mas é bom tratar)
    /// TODO: Test with exclusive lock mode, reusing connection
    /// </summary>
    public class SdbQueue : IDisposable
    {
        private readonly BlockingCollection<SdbRequest> rwCommandsQueue;

        //public Task WorkerTask;
        private readonly SdbConnection conn;
        private bool disposedValue;
        // public long CurrentSize = 0;
        public int RetryAttemptsWithConnectionFailure = 10000;

        public Action<string> OnLog = null;
        public Action OnQueryRunnerStopped = null;


        private readonly object _sync = new object();
        public bool QueueProcessingIsRunning { get; private set; } = false;

        public SdbQueue(SdbConnection conn, int queueSize = 400)
        {
            // roCommandsQueue = rwCommandsQueue;
            this.rwCommandsQueue = new BlockingCollection<SdbRequest>(queueSize);
            this.conn = conn;
        }

        public Task<SdbResult> ExecuteAsync(SdbRequest dbRequest)
        {
            dbRequest.TaskCompletionSource = new TaskCompletionSource<SdbResult>();
            Enqueue(dbRequest);
            return dbRequest.TaskCompletionSource.Task;
        }

        public SdbResult Execute(SdbRequest dbRequest)
        {
            dbRequest.TaskCompletionSource = new TaskCompletionSource<SdbResult>();
            Enqueue(dbRequest);
            return dbRequest.TaskCompletionSource.Task.GetAwaiter().GetResult();
        }

        public int QueueSize()
        {
            return rwCommandsQueue.Count;

        }
        private void Enqueue(SdbRequest dbRequest)
        {
            //if (dbRequest.Type == DbRequest.CmdType.Query || dbRequest.Type == DbRequest.CmdType.SingleResult)
            //    roCommandsQueue.Add(dbRequest);
            //else
            rwCommandsQueue.Add(dbRequest);
        }

        public Task StartProcessItems(int batchSize = 20)
        {
            return Task.Run(() =>
            {
                try
                {
                    QueueProcessingIsRunning = true;
                    conn.InitializeFull();
                    conn.Open();
                    ProcessQueueItem(batchSize);
                    OnLog?.Invoke($"ProcessQueueItem {conn.dbConfig.Name} stopped");
                    conn.Close();
                }
                catch (Exception ex)
                {
                    OnLog?.Invoke($"Fatal error, StartProcessItems {conn.dbConfig.Name} stopped: {ex}");
                }
                finally
                {
                    QueueProcessingIsRunning = false;
                    OnQueryRunnerStopped?.Invoke();
                }
            });
        }

        private void ProcessQueueItem(int batchSize)
        {
            int cntErros = 0;

            SdbRequest[] reqBatchSolo = new SdbRequest[1];
            Stopwatch stopwatch = new Stopwatch();

            while (!disposedValue)
            {
                try
                {
                    if (!conn.Initialized)
                    {
                        Task.Delay(100).Wait();
                        continue;
                    }

                    stopwatch.Restart();

                    if (rwCommandsQueue.Count > 4)
                    {
                        //Se tem mais de 4 pega lotes de {batchSize} e executa dentro de uma transação isolada, em outra thread, mas com lock exclusivo
                        ExecuteBatch(rwCommandsQueue, batchSize);
                    }
                    else if (rwCommandsQueue.TryTake(out SdbRequest dbRequest, conn.IsOpen() ? 2000 : 60000))
                    {
                        //Few records, run each alone

                        reqBatchSolo[0] = dbRequest;
                        ExecuteDbRequest(reqBatchSolo);

                        //OnLog?.Invoke($"{log} {lw.Elapsed}ms");
                        cntErros = 0;
                    }
                    else
                    {
                        conn.TryClose();
                    }
                }
                catch (ObjectDisposedException)
                {
                    OnLog?.Invoke($"ObjectDisposedException {conn?.dbConfig.Name}");
                }
                catch (Exception ex)
                {
                    OnLog?.Invoke($"DbQueue error (some commands were lost): {ex} ");
                    if (cntErros > RetryAttemptsWithConnectionFailure)
                        throw;

                    try
                    {
                        conn.Close();
                        Task.Delay(400).Wait();
                        conn.Open();
                    }
                    catch (Exception exRecon)
                    {
                        OnLog?.Invoke($"Recon  error (some commands were lost): {exRecon}");
                    }

                    cntErros++;
                }
            }
        }

        private void ExecuteBatch(BlockingCollection<SdbRequest> commandQueue, int count)
        {

            Stopwatch stopwatch = new Stopwatch();


            SdbRequest[] reqBatch = new SdbRequest[count];
            int i;
            for (i = 0; i < count; i++)
            {
                if (commandQueue.TryTake(out SdbRequest dbRequest1))
                {
                    reqBatch[i] = dbRequest1;
                }
                else
                {
                    reqBatch[i] = null;
                    break;
                }
            }

            int remaining = commandQueue.Count;

            ExecuteDbRequest(reqBatch);

            if (remaining > 30)
            {
                OnLog?.Invoke($"{conn.dbConfig.Name} run:{i} remaining:{remaining} {stopwatch.ElapsedMilliseconds}ms");
            }

        }

        private void ExecuteDbRequest(SdbRequest[] dbRequests)
        {

            DbTransaction tx = null;
            try
            {
                Monitor.Enter(_sync);
                conn.LockConnection();
                conn.Open();


                tx = conn.BeginTransaction();


                foreach (var dbRequest in dbRequests)
                {
                    // End of valid values
                    if (dbRequest == null)
                        break;

                    try
                    {
                        //  conn.Close();
                        var result = SdbRequestRunner.ExecuteDbRequest(conn.Connection, dbRequest, conn.dbConfig);
                        if (!dbRequest.TaskCompletionSource.TrySetResult(result))
                        {
                            OnLog?.Invoke("Wasn't able to set result");
                        }

                    }

                    catch (Exception e)
                    {
                        OnLog?.Invoke($"TaskCompletionSource.SetException {e}");
                        //REMEMBER: Em caso de exceção a requisição irá se perder, cabe ao solicitador reenviar a requisição
                        dbRequest.TaskCompletionSource.TrySetException(e);
                    }

                }
                tx?.Commit();
            }
            catch (Exception ex)
            {
                OnLog?.Invoke($"ExecuteDbRequest {ex}");

                try
                {
                    // Mesmo com exceção
                    tx?.Commit();
                }
                catch
                {
                }
                try
                {
                    conn.RecreateConnection();
                }
                catch (Exception exre)
                {
                    OnLog?.Invoke($"Trying to recreate connection failed: {exre}");
                }
            }
            finally
            {
                conn.ReleaseConnection();
                Monitor.Exit(_sync);
            }

        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    rwCommandsQueue.Dispose();
                    //  roCommandsQueue.Dispose();
                    conn.Close();
                    // readerWriterLockSlim.Dispose();
                }

                //  free unmanaged resources (unmanaged objects) and override finalizer
                //  set large fields to null
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }

}

