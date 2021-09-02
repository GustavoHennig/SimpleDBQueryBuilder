using System;
using System.Collections.Generic;

namespace GHSoftware.SimpleDb
{
    public class DbRequestBuilder<TFieldEnumType>
    {
        private List<string> SelectFields = null;
        private List<KeyValuePair<string, object>> InsertUpdateValues = null;
        private List<(string key, object value, string op)> Where = null;
        private string WhereSql = null;
        private string OrderCriteria = null;
        private string AppendSql = null;
        private string tableName;
        private bool IsScalar = false;
        private string FieldQuotes = "";

        public static DbRequestBuilder<T> Of<T>()
        {
            DbRequestBuilder<T> dbRequestBuilder = new DbRequestBuilder<T>();
            return dbRequestBuilder;
        }

        //public static DbRequestBuilder<TFieldEnumType> Of<TFieldEnumType>()
        //{
        //    DbRequestBuilder<TFieldEnumType> dbRequestBuilder = new DbRequestBuilder<TFieldEnumType>();
        //    return dbRequestBuilder;
        //}

        public DbRequestBuilder<TFieldEnumType> WithSelectFields(params string[] fields)
        {
            if (SelectFields == null) SelectFields = new List<string>();
            SelectFields.AddRange(fields);
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithSelectFields<T>(params T[] fields) where T : Enum
        {
            if (SelectFields == null) SelectFields = new List<string>();
            foreach (var en in fields)
                SelectFields.Add(en.ToString());
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithSelectFields(params TFieldEnumType[] fields)
        {
            if (SelectFields == null) SelectFields = new List<string>();
            foreach (var en in fields)
                SelectFields.Add(en.ToString());
            return this;
        }


        public DbRequestBuilder<TFieldEnumType> WithFieldValues(List<KeyValuePair<string, object>> fieldValues)
        {
            if (InsertUpdateValues == null) InsertUpdateValues = new List<KeyValuePair<string, object>>();

            InsertUpdateValues.AddRange(fieldValues);
            return this;
        }
        public DbRequestBuilder<TFieldEnumType> WithFieldValues(string key, object value)
        {
            if (InsertUpdateValues == null) InsertUpdateValues = new List<KeyValuePair<string, object>>();
            InsertUpdateValues.Add(new KeyValuePair<string, object>(key, value));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> AppendToQuery(string sql)
        {
            AppendSql = sql;
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithFieldQuotes(string quotes = "\"")
        {
            FieldQuotes = quotes;
            return this;
        }


        public DbRequestBuilder<TFieldEnumType> WithFieldValues<T>(T key, object value) where T : Enum
        {
            if (InsertUpdateValues == null) InsertUpdateValues = new List<KeyValuePair<string, object>>();
            InsertUpdateValues.Add(new KeyValuePair<string, object>(key.ToString(), value));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithFieldValues(TFieldEnumType key, object value)
        {
            if (InsertUpdateValues == null) InsertUpdateValues = new List<KeyValuePair<string, object>>();
            InsertUpdateValues.Add(new KeyValuePair<string, object>(key.ToString(), value));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond(string key, object value, string oper = "=")
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key, value, oper));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond(string sqlWhereCond)
        {
            WhereSql = sqlWhereCond;
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond<T>(T key, object value, string oper = "=") where T : Enum
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key.ToString(), value, oper));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond(TFieldEnumType key, object value, string oper = "=")
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key.ToString(), value, oper));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithTable(string tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> SetScalar(bool scalar)
        {
            this.IsScalar = scalar;
            return this;
        }


        public DbRequestBuilder<TFieldEnumType> SetOrder(string orderCriteria)
        {
            OrderCriteria = orderCriteria;
            return this;
        }

        public DbRequest BuildSelect()
        {

            string sql = "select ";


            sql += FieldQuotes + string.Join($"{FieldQuotes},{FieldQuotes}", SelectFields) + FieldQuotes;
            sql += $" from {FieldQuotes}{tableName}{FieldQuotes}";


            if (!string.IsNullOrEmpty(WhereSql))
            {
                sql += " where " + WhereSql;
            }
            else if (Where?.Count > 0)
            {
                sql += " where ";

                foreach (var (key, value, op) in Where)
                {
                    sql += $"{FieldQuotes}{key}{FieldQuotes} {op} @{key} and ";
                }
                sql = sql.Remove(sql.Length - 4);
            }


            if (!string.IsNullOrEmpty(OrderCriteria))
            {
                sql += " order by " + OrderCriteria;
            }
            if (AppendSql != null)
            {
                sql += AppendSql;
            }
            DbRequest dbRequest = DbRequest.Of(
                IsScalar ? DbRequest.CmdType.SingleResult : DbRequest.CmdType.Query,
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
                sqlf += FieldQuotes + kv.Key + FieldQuotes + ",";
                sqlv += "@" + kv.Key + ",";
            }

            sqlf = sqlf.Remove(sqlf.Length - 1);
            sqlv = sqlv.Remove(sqlv.Length - 1);



            string sql = $"insert into {FieldQuotes}{tableName}{FieldQuotes} ({sqlf}) values ({sqlv}) ";
            if (AppendSql != null)
            {
                sql += AppendSql;
            }


            DbRequest dbRequest = DbRequest.Of(
                DbRequest.CmdType.CommandWithIdentity,
                sql,
                null,
                InsertUpdateValues);

            return dbRequest;
        }

        public DbRequest BuildUpdate()
        {
            string sql = $"update {FieldQuotes}{tableName}{FieldQuotes} set ";

            foreach (var kv in InsertUpdateValues)
            {
                sql += FieldQuotes + kv.Key + FieldQuotes+"=@" + kv.Key + ",";
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
                    sql += $"{FieldQuotes}{key}{FieldQuotes} {op} @{key} ";
                }
            }
            if (AppendSql != null)
            {
                sql += AppendSql;
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
            string sql = $"delete from {FieldQuotes}{tableName}{FieldQuotes} where ";

            if (!string.IsNullOrEmpty(WhereSql))
            {
                sql += WhereSql;
            }
            else
            {
                foreach (var (key, value, op) in Where)
                {
                    sql += $"{FieldQuotes}{key}{FieldQuotes} {op} @{key} ";
                }
            }

            if (AppendSql != null)
            {
                sql += AppendSql;
            }

            DbRequest dbRequest = DbRequest.Of(
               DbRequest.CmdType.Command,
               sql);

            if (Where != null)
                dbRequest.AddRange(Where);

            return dbRequest;
        }




    }

    public class DbRequestBuilder : DbRequestBuilder<Enum>
    {
        public static DbRequestBuilder Of()
        {
            DbRequestBuilder dbRequestBuilder = new DbRequestBuilder();
            return dbRequestBuilder;
        }
    }
}
