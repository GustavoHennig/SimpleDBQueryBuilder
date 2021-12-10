using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Text;

namespace GHSoftware.SimpleDb
{
    internal class SdbMigrationRunner
    {
        public static void RunMigrations(DbConnection conn, SdbMigrationConfig dbMigrationConfig)
        {


            IDbTransaction dbTransaction = null;
            try
            {
                dbTransaction = conn.BeginTransaction();

                int userVersion = dbMigrationConfig.MigrationCommands.GetSchemaVersion(conn);
                int i;

                using (var com = conn.CreateCommand())
                {

                    for (i = userVersion; i < dbMigrationConfig.Migrations.Count; i++)
                    {
                        try
                        {
                            com.CommandText = dbMigrationConfig.Migrations[i];
                            com.ExecuteNonQuery();
                        }
                        catch (DbException ex)
                        {
                            if (ex.ErrorCode != 1)
                                throw;
                        }
                    }
                }

                dbMigrationConfig.MigrationCommands.SetSchemaVersion(conn, i);


                dbTransaction.Commit();
            }
            catch (Exception)
            {
                dbTransaction?.Rollback();
            }
        }
    }
}
