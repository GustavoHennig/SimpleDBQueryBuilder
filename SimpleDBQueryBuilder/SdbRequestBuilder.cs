using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GHSoftware.SimpleDb
{
    public class SdbRequestBuilder<TFieldEnumType>
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

        public static SdbRequestBuilder<T> Of<T>()
        {
            SdbRequestBuilder<T> dbRequestBuilder = new SdbRequestBuilder<T>();
            return dbRequestBuilder;
        }

        //public static DbRequestBuilder<TFieldEnumType> Of<TFieldEnumType>()
        //{
        //    DbRequestBuilder<TFieldEnumType> dbRequestBuilder = new DbRequestBuilder<TFieldEnumType>();
        //    return dbRequestBuilder;
        //}



        public SdbRequestBuilder<TFieldEnumType> WithSelectFields(params string[] fields)
        {
            if (_selectFields == null) _selectFields = new List<string>();
            _selectFields.AddRange(fields);
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WithSelectFields<T>(params T[] fields) where T : Enum
        {
            if (_selectFields == null) _selectFields = new List<string>();
            foreach (var en in fields)
                _selectFields.Add(en.ToString());
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WithSelectFields(params TFieldEnumType[] fields)
        {
            if (_selectFields == null) _selectFields = new List<string>();
            foreach (var en in fields)
                _selectFields.Add(en.ToString());
            return this;
        }


        public SdbRequestBuilder<TFieldEnumType> WithFieldValues(List<KeyValuePair<string, object>> fieldValues)
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();

            _insertUpdateValues.AddRange(fieldValues.Select(s => new KeyValuePair<string, object>(s.Key, s.Value)));
            return this;
        }
        public SdbRequestBuilder<TFieldEnumType> WithFieldValues(string key, object value)
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();
            _insertUpdateValues.Add(new KeyValuePair<string, object>(key, value));
            return this;
        }

        //public DbRequestBuilder<TFieldEnumType> WithFieldValues(string rawFieldUpdateSql, object value, string parameterName)
        //{
        //    if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();
        //    _insertUpdateValues.Add(new InsertUpdateFieldValue(parameterName, value, rawFieldUpdateSql));
        //    return this;
        //}


        public SdbRequestBuilder<TFieldEnumType> AppendToQuery(string sql)
        {
            _appendSql = sql;
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WithFieldQuotes(string quotes = "\"")
        {
            _fieldQuotes = quotes;
            return this;
        }


        public SdbRequestBuilder<TFieldEnumType> WithFieldValues<T>(T key, object value) where T : Enum
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();
            _insertUpdateValues.Add(new KeyValuePair<string, object>(key.ToString(), value));
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WithFieldValues(TFieldEnumType key, object value)
        {
            if (_insertUpdateValues == null) _insertUpdateValues = new List<KeyValuePair<string, object>>();
            _insertUpdateValues.Add(new KeyValuePair<string, object>(key.ToString(), value));
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WhereCond(string key, object value, string oper = "=")
        {
            WhereBuilder.WhereCond(key, value, oper);
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WhereCond(string sqlWhereCond)
        {
            WhereBuilder.WhereCond(sqlWhereCond);
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WhereCond<T>(T key, object value, string oper = "=") where T : Enum
        {
            WhereBuilder.WhereCond(key, value, oper);
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WhereCond(TFieldEnumType key, object value, string oper = "=")
        {
            WhereBuilder.WhereCond(key, value, oper);
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WhereCondIn(TFieldEnumType key, object[] values)
        {
            WhereBuilder.WhereCondIn(key, values);
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> WithTable(string tableName)
        {
            this._tableName = tableName;
            return this;
        }

        public SdbRequestBuilder<TFieldEnumType> SetScalar(bool scalar)
        {
            this._isScalar = scalar;
            return this;
        }


        public SdbRequestBuilder<TFieldEnumType> SetOrder(string orderCriteria)
        {
            _orderCriteria = orderCriteria;
            return this;
        }

        public SdbRequest BuildSelect()
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
            SdbRequest dbRequest = SdbRequest.Of(
                _isScalar ? SdbRequest.CmdType.SingleResult : SdbRequest.CmdType.Query,
                sql,
                this._selectFields.ToArray()
                );

            if (_whereBuilder != null)
            {
                dbRequest.AddParamRange(WhereBuilder.GetParameters());
            }

            return dbRequest;
        }


        public SdbRequest BuildInsert()
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


            SdbRequest dbRequest = SdbRequest.Of(
                SdbRequest.CmdType.CommandWithIdentity,
                sql,
                null,
                _insertUpdateValues);

            return dbRequest;
        }

        public SdbRequest BuildUpdate(string customFieldsUpdate = "")
        {
            StringBuilder sql = new StringBuilder($"update {_fieldQuotes}{_tableName}{_fieldQuotes} set ");

            if (!string.IsNullOrEmpty(customFieldsUpdate))
            {
                sql.Append(customFieldsUpdate);
            }

            foreach (var kv in _insertUpdateValues)
            {
                sql.Append(_fieldQuotes + kv.Key + _fieldQuotes + "=@" + kv.Key + ",");
            }

            sql.Remove(sql.Length - 1, 1);


            if (_whereBuilder != null)
            {
                sql.Append(WhereBuilder.Build());
            }
            else
            {
                throw new ArgumentException("WHERE condition is mandatory, you can override with 1=1");
            }

            if (_appendSql != null)
            {
                sql.Append(_appendSql);
            }
            SdbRequest dbRequest = SdbRequest.Of(
               SdbRequest.CmdType.Command,
               sql.ToString(),
               null,
               _insertUpdateValues);

            dbRequest.AddParamRange(WhereBuilder.GetParameters());

            return dbRequest;
        }

        public SdbRequest BuildDelete()
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

            SdbRequest dbRequest = SdbRequest.Of(
               SdbRequest.CmdType.Command,
               sql);

            dbRequest.AddParamRange(WhereBuilder.GetParameters());

            return dbRequest;
        }

        private class InsertUpdateFieldValue
        {
            public string Field;
            public object Value;
            /// <summary>
            /// Optional sql
            /// </summary>
            public string RawSql;


            /// <summary>
            /// 
            /// </summary>
            /// <param name="field"></param>
            /// <param name="value"></param>
            /// <param name="rawSql">Optional, replaces fiela by an expression</param>
            public InsertUpdateFieldValue(string field, object value, string rawSql = null)
            {
                this.Field = field;
                this.Value = value;
                this.RawSql = rawSql;
            }

            //TODO: Create factory with a clear signature.
            // Ex. CreateFieldValue
            // Ex. CreateExpressionWithParameterValue

        }
    }

    public class SdbRequestBuilder
        : SdbRequestBuilder
        <Enum>
    {
        public static SdbRequestBuilder Of()
        {
            SdbRequestBuilder dbRequestBuilder = new SdbRequestBuilder();
            return dbRequestBuilder;
        }
    }
}
