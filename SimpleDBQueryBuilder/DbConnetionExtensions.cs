using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Text;

namespace GHSoftware.SimpleDb
{
    public static class DbConnetionExtensions
    {

        /// <summary>
        /// Executa uma consulta SQL
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static CommandResult RunQuery(this DbConnection dbConnection, string sql)
        {

            DbCommand cmd = dbConnection.CreateCommand();
            cmd.CommandText = sql;


            IDataReader dr = null;
            try
            {
                dr = cmd.ExecuteReader();
            }
            catch (Exception)
            {
                throw;
            }


            return new CommandResult(dr, cmd);

        }


        public static CommandResult RunQueryWithParameters(this DbConnection dbConnection, string sql, NameValueCollection parameters)
        {
            DbCommand cmd = dbConnection.CreateCommand();
            cmd.CommandText = sql;

            foreach (var key in parameters.AllKeys)
            {
                DbParameter p = cmd.CreateParameter();
                p.ParameterName = key;
                p.Value = parameters[key];
                if (p.Value == null)
                    p.Value = DBNull.Value;

                cmd.Parameters.Add(p);
            }

            IDataReader dr;
            try
            {
                dr = cmd.ExecuteReader();
            }
            catch (Exception)
            {
                throw;
            }

            return new CommandResult(dr, cmd);

        }

        /// <summary>
        /// Executa um comando SQL
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="GetIdentity"></param>
        /// <returns></returns>
        public static long RunSql(this DbConnection dbConnection, string sql)
        {
            using (DbCommand cmd = dbConnection.CreateCommand())
            {
                cmd.CommandText = sql;

                return cmd.ExecuteNonQuery();
            }
        }

        public static long RunSqlScalar(this DbConnection dbConnection, string sql)
        {
            using (DbCommand cmd = dbConnection.CreateCommand())
            {
                cmd.CommandText = sql;

                return Convert.ToInt32(cmd.ExecuteScalar());
            }
        }
    }
}
