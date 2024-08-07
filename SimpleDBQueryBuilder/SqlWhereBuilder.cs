using System;
using System.Collections.Generic;
using System.Text;

namespace GHSoftware.SimpleDb
{
    internal class SqlWhereBuilder<TFieldEnumType>
    {
        private readonly string FieldQuotes;
        private string WhereSql = null;
        private List<(string key, object value, string op)> Where = null;

        public SqlWhereBuilder(string fieldQuotes)
        {
            FieldQuotes = fieldQuotes;
        }

        public SqlWhereBuilder<TFieldEnumType> WhereCond(string key, object value, string oper = "=")
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key, value, oper));
            return this;
        }

        public SqlWhereBuilder<TFieldEnumType> WhereCond(string sqlWhereCond)
        {
            WhereSql = sqlWhereCond;
            return this;
        }

        public SqlWhereBuilder<TFieldEnumType> WhereCond<T>(T key, object value, string oper = "=") where T : Enum
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key.ToString(), value, oper));
            return this;
        }

        public SqlWhereBuilder<TFieldEnumType> WhereCond(TFieldEnumType key, object value, string oper = "=")
        {
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key.ToString(), value, oper));
            return this;
        }

        public SqlWhereBuilder<TFieldEnumType> WhereCondIn(TFieldEnumType key, object[] values)
        {
            if(values.Length == 0) return this;
            if (Where == null) Where = new List<(string key, object value, string op)>();
            Where.Add((key.ToString(), values, "in"));
            return this;
        }

        public string Build()
        {

            if (string.IsNullOrEmpty(WhereSql) && Where?.Count == 0)
            {
                return "";
            }

            StringBuilder sql = new StringBuilder(40);

            sql.Append(" where ");


            if (!string.IsNullOrEmpty(WhereSql))
            {
                sql.Append(WhereSql);
            }
            else if (Where?.Count > 0)
            {

                foreach (var (key, value, op) in Where)
                {
                    if (op == "in")
                    {
                        sql.Append($"{FieldQuotes}{key}{FieldQuotes} {op} (");
                        int cnt = 0;
                        foreach (var par in (IEnumerable<object>)value)
                        {
                            sql.Append($"@{key}_p{cnt},");
                            cnt++;
                        }
                        sql.Remove(sql.Length - 1, 1);
                        sql.Append($") and ");
                    }
                    else
                    {
                        sql.Append($"{FieldQuotes}{key}{FieldQuotes} {op} @{key} and ");
                    }
                }
                sql = sql.Remove(sql.Length - 4, 4);
            }

            return sql.ToString();
        }

        public IEnumerable<KeyValuePair<string, object>> GetParameters()
        {

            if (Where != null)
            {
                foreach (var (key, value, op) in Where)
                {
                    if (op == "in")
                    {
                        int cnt = 0;
                        foreach (var par in (IEnumerable<object>)value)
                        {
                            yield return new KeyValuePair<string, object>($"{key}_p{cnt}", par);
                            cnt++;
                        }
                    }
                    else
                    {
                        yield return new KeyValuePair<string, object>(key, value);
                    }
                }
            }
        }
    }
}
