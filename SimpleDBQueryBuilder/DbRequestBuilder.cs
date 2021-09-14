using System;
using System.Collections.Generic;

namespace GHSoftware.SimpleDb
{
    public class DbRequestBuilder<TFieldEnumType>
    {
        /// <summary>
        /// TODO: Extract it to a separate class: SqlSelectBuilder
        /// </summary>
        private List<string> _selectFields = null;

        /// <summary>
        /// TODO: Think if should extract it to a separate class: SqlInsertUpdateBuilder
        /// </summary>
        private List<KeyValuePair<string, object>> _insertUpdateValues = null;
        private SqlWhereBuilder<TFieldEnumType> _whereBuilder = null;
        private string _orderCriteria = null;
        private string _appendSql = null;
        private string _tableName;
        private bool _isScalar = false;
        private string _fieldQuotes = "";

        private SqlWhereBuilder<TFieldEnumType> WhereBuilder
        {
            get
            {
                if (_whereBuilder == null)
                {
                    _whereBuilder = new SqlWhereBuilder<TFieldEnumType>(_fieldQuotes);
                }
                return _whereBuilder;
            }
        }

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
            if (_selectFields == null) _selectFields = new List<string>();
            _selectFields.AddRange(fields);
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithSelectFields<T>(params T[] fields) where T : Enum
        {
            if (_selectFields == null) _selectFields = new List<string>();
            foreach (var en in fields)
                _selectFields.Add(en.ToString());
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithSelectFields(params TFieldEnumType[] fields)
        {
            if (_selectFields == null) _selectFields = new List<string>();
            foreach (var en in fields)
                _selectFields.Add(en.ToString());
            return this;
        }


        public DbRequestBuilder<TFieldEnumType> WithFieldValues(List<KeyValuePair<string, object>> fieldValues)
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();

            _insertUpdateValues.AddRange(fieldValues);
            return this;
        }
        public DbRequestBuilder<TFieldEnumType> WithFieldValues(string key, object value)
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();
            _insertUpdateValues.Add(new KeyValuePair<string, object>(key, value));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> AppendToQuery(string sql)
        {
            _appendSql = sql;
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithFieldQuotes(string quotes = "\"")
        {
            _fieldQuotes = quotes;
            return this;
        }


        public DbRequestBuilder<TFieldEnumType> WithFieldValues<T>(T key, object value) where T : Enum
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();
            _insertUpdateValues.Add(new KeyValuePair<string, object>(key.ToString(), value));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithFieldValues(TFieldEnumType key, object value)
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();
            _insertUpdateValues.Add(new KeyValuePair<string, object>(key.ToString(), value));
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond(string key, object value, string oper = "=")
        {
            WhereBuilder.WhereCond(key, value, oper);
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond(string sqlWhereCond)
        {
            WhereBuilder.WhereCond(sqlWhereCond);
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond<T>(T key, object value, string oper = "=") where T : Enum
        {
            WhereBuilder.WhereCond(key, value, oper);
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCond(TFieldEnumType key, object value, string oper = "=")
        {
            WhereBuilder.WhereCond(key, value, oper);
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WhereCondIn(TFieldEnumType key, object[] values)
        {
            WhereBuilder.WhereCondIn(key, values);
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> WithTable(string tableName)
        {
            this._tableName = tableName;
            return this;
        }

        public DbRequestBuilder<TFieldEnumType> SetScalar(bool scalar)
        {
            this._isScalar = scalar;
            return this;
        }


        public DbRequestBuilder<TFieldEnumType> SetOrder(string orderCriteria)
        {
            _orderCriteria = orderCriteria;
            return this;
        }

        public DbRequest BuildSelect()
        {

            string sql = "select ";


            if (_selectFields == null)
            {
                //TODO: shouldn't this case be "select * from table" ?
                throw new ArgumentException("You must inform some select fields before building the SQL");
            }

            sql += _fieldQuotes + string.Join($"{_fieldQuotes},{_fieldQuotes}", _selectFields) + _fieldQuotes;
            sql += $" from {_fieldQuotes}{_tableName}{_fieldQuotes}";

            if (_whereBuilder != null)
            {
                sql += WhereBuilder.Build();
            }

            if (!string.IsNullOrEmpty(_orderCriteria))
            {
                sql += " order by " + _orderCriteria;
            }
            if (_appendSql != null)
            {
                sql += _appendSql;
            }
            DbRequest dbRequest = DbRequest.Of(
                _isScalar ? DbRequest.CmdType.SingleResult : DbRequest.CmdType.Query,
                sql,
                this._selectFields.ToArray()
                );

            if (_whereBuilder != null)
            {
                dbRequest.AddRange(WhereBuilder.GetParameters());
            }

            return dbRequest;
        }


        public DbRequest BuildInsert()
        {

            string sqlf = "";
            string sqlv = "";

            foreach (var kv in _insertUpdateValues)
            {
                sqlf += _fieldQuotes + kv.Key + _fieldQuotes + ",";
                sqlv += "@" + kv.Key + ",";
            }

            sqlf = sqlf.Remove(sqlf.Length - 1);
            sqlv = sqlv.Remove(sqlv.Length - 1);



            string sql = $"insert into {_fieldQuotes}{_tableName}{_fieldQuotes} ({sqlf}) values ({sqlv}) ";
            if (_appendSql != null)
            {
                sql += _appendSql;
            }


            DbRequest dbRequest = DbRequest.Of(
                DbRequest.CmdType.CommandWithIdentity,
                sql,
                null,
                _insertUpdateValues);

            return dbRequest;
        }

        public DbRequest BuildUpdate()
        {
            string sql = $"update {_fieldQuotes}{_tableName}{_fieldQuotes} set ";

            foreach (var kv in _insertUpdateValues)
            {
                sql += _fieldQuotes + kv.Key + _fieldQuotes + "=@" + kv.Key + ",";
            }

            sql = sql.Remove(sql.Length - 1);

            if (_whereBuilder != null)
            {
                sql += WhereBuilder.Build();
            }
            else
            {
                throw new ArgumentException("WHERE condition is mandatory, you can override with 1=1");
            }

            if (_appendSql != null)
            {
                sql += _appendSql;
            }
            DbRequest dbRequest = DbRequest.Of(
               DbRequest.CmdType.Command,
               sql,
               null,
               _insertUpdateValues);

            dbRequest.AddRange(WhereBuilder.GetParameters());

            return dbRequest;
        }

        public DbRequest BuildDelete()
        {
            string sql = $"delete from {_fieldQuotes}{_tableName}{_fieldQuotes} ";

            if (_whereBuilder != null)
            {
                sql += WhereBuilder.Build();
            }
            else
            {
                throw new ArgumentException("WHERE condition is mandatory, you can override with 1=1");
            }

            if (_appendSql != null)
            {
                sql += _appendSql;
            }

            DbRequest dbRequest = DbRequest.Of(
               DbRequest.CmdType.Command,
               sql);

            dbRequest.AddRange(WhereBuilder.GetParameters());

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
