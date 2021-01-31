using System;
using System.Collections.Generic;

namespace GHSoftware.SimpleDb
{
    public class DbRequestBuilder
    {
        private List<string> SelectFields = null;
        private List<KeyValuePair<string, object>> InsertUpdateValues = null;
        private List<(string key, object value, string op)> Where = null;
        private string WhereSql = null;
        private string tableName;

        public static DbRequestBuilder Of()
        {
            DbRequestBuilder DbRequestBuilder = new DbRequestBuilder();
            return DbRequestBuilder;
        }

        public DbRequestBuilder WithSelectFields(params string[] fields)
        {
            if (SelectFields == null) SelectFields = new List<string>();
            SelectFields.AddRange(fields);
            return this;
        }

        public DbRequestBuilder WithSelectFields<T>(params T[] fields) where T : Enum
        {
            if (SelectFields == null) SelectFields = new List<string>();
            foreach (var en in fields)
                SelectFields.Add(en.ToString());
            return this;
        }

        public DbRequestBuilder WithFieldValues(List<KeyValuePair<string, object>> fieldValues)
        {
            if (InsertUpdateValues == null) InsertUpdateValues = new List<KeyValuePair<string, object>>();

            InsertUpdateValues.AddRange(fieldValues);
            return this;
        }
        public DbRequestBuilder WithFieldValues(string key, object value)
        {
            if (InsertUpdateValues == null) InsertUpdateValues = new List<KeyValuePair<string, object>>();
            InsertUpdateValues.Add(new KeyValuePair<string, object>(key, value));
            return this;
        }

        public DbRequestBuilder WithFieldValues<T>(T key, object value) where T : Enum
        {
            if (InsertUpdateValues == null) InsertUpdateValues = new List<KeyValuePair<string, object>>();
            InsertUpdateValues.Add(new KeyValuePair<string, object>(key.ToString(), value));
            return this;
        }

        public DbRequestBuilder WhereCond(string key, object value, string oper = "=")
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key, value, oper));
            return this;
        }

        public DbRequestBuilder WhereCond(string sqlWhereCond)
        {
            WhereSql = sqlWhereCond;
            return this;
        }

        public DbRequestBuilder WhereCond<T>(T key, object value, string oper = "=") where T : Enum
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key.ToString(), value, oper));
            return this;
        }

        public DbRequestBuilder WithTable(string tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public DbRequest BuildSelect()
        {

            string sql = "select ";


            sql += string.Join(",", SelectFields);
            sql += $" from {tableName}";


            if (!string.IsNullOrEmpty(WhereSql))
            {
                sql += " where " + WhereSql;
            }
            else if (Where?.Count > 0)
            {
                sql += " where ";

                foreach (var (key, value, op) in Where)
                {
                    sql += $"{key} {op} @{key} and ";
                }
                sql = sql.Remove(sql.Length - 4);
            }

            DbRequest dbRequest = DbRequest.Of(
                DbRequest.CmdType.Query,
                sql,
                this.SelectFields.ToArray()
                );

            if (Where?.Count > 0)
                dbRequest.AddRange(Where);


            return dbRequest;
        }


        public DbRequest BuildInsert()
        {

            string sqlf = "";
            string sqlv = "";

            foreach (var kv in InsertUpdateValues)
            {
                sqlf += kv.Key + ",";
                sqlv += "@" + kv.Key + ",";
            }

            sqlf = sqlf.Remove(sqlf.Length - 1);
            sqlv = sqlv.Remove(sqlv.Length - 1);

            DbRequest dbRequest = DbRequest.Of(
                DbRequest.CmdType.CommandWithIdentity,
                $"insert into {tableName} ({sqlf}) values ({sqlv}) ",
                null,
                InsertUpdateValues);

            return dbRequest;
        }

        public DbRequest BuildUpdate()
        {
            string sql = $"update {tableName} set ";

            foreach (var kv in InsertUpdateValues)
            {
                sql += kv.Key + "=@" + kv.Key + ",";
            }

            sql = sql.Remove(sql.Length - 1);
            sql += " where ";

            if (!string.IsNullOrEmpty(WhereSql))
            {
                sql += WhereSql;
            }
            else
            {
                foreach (var (key, value, op) in Where)
                {
                    sql += $"{key} {op} @{key} ";
                }
            }

            DbRequest dbRequest = DbRequest.Of(
               DbRequest.CmdType.Command,
               sql,
               null,
               InsertUpdateValues);

            if (Where != null)
                dbRequest.AddRange(Where);

            return dbRequest;
        }

        public DbRequest BuildDelete()
        {
            string sql = $"delete from {tableName} where ";

            if (!string.IsNullOrEmpty(WhereSql))
            {
                sql += WhereSql;
            }
            else
            {
                foreach (var (key, value, op) in Where)
                {
                    sql += $"{key} {op} @{key} ";
                }
            }

            DbRequest dbRequest = DbRequest.Of(
               DbRequest.CmdType.Command,
               sql);

            if (Where != null)
                dbRequest.AddRange(Where);

            return dbRequest;
        }




    }
}
