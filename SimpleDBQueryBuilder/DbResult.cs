using System;
using System.Collections.Generic;
using System.Data;

namespace GHSoftware.SimpleDb
{

    /// <summary>
    /// It is a lightweight DataTable and offline DataReader :)
    /// </summary>
    public class DbResult
    {
        public List<object[]> RawResult = null;
        public Dictionary<string, int> ColumnsHash = null;
        public int ColumnsCount;


        public DbResult(int columnsCount)
        {
            ColumnsCount = columnsCount;
            RawResult = new List<object[]>();
        }

        public DbResult(string[] columns = null)
        {
            RawResult = new List<object[]>();

            ColumnsCount = columns.Length;
            ColumnsHash = new Dictionary<string, int>(ColumnsCount);
            for (int i = 0; i < ColumnsCount; i++)
            {
                ColumnsHash.Add(columns[i], i);
            }
        }
        public T GetValue<T>(object[] row, string columnName)
        {
            return ConvertValue<T>(row[ColumnsHash[columnName]]);
        }

        public T GetValue<T>(object[] row, Enum column)
        {
            return GetValue<T>(row, column.ToString());
        }

        public T GetValue<T>(int rowIndex, string columnName)
        {
            return ConvertValue<T>(RawResult[rowIndex][ColumnsHash[columnName]]);
        }

        public T GetValue<T>(int rowIndex, int columnIndex)
        {
            return ConvertValue<T>(RawResult[rowIndex][columnIndex]);
        }

        private T ConvertValue<T>(object obj)
        {

            if (obj == null || obj is DBNull)
            {
                return default(T);
            }
            else if (typeof(T) == typeof(DateTime?))
            {
                string s = ConvertValue<string>(obj);

                if (DateTime.TryParse(s, out DateTime dt))
                {
                    return (T)(object)dt;
                }
                else
                {
                    return default(T);
                }
            }
            else if (typeof(T) == typeof(long))
            {
                return (T)(object)Convert.ToInt64(obj);
        }
            else if (typeof(T) == typeof(int))
        {
                return (T)(object)Convert.ToInt32(obj);
        }
            else if (typeof(T).IsEnum)
        {
                return (T)Enum.Parse(typeof(T), obj.ToString());
        }
            else
        {
                return (T)obj;
            }
        }

        public void AddSingle(object value)
        {
            RawResult.Add(new object[] { value });
        }
        public void AddRow(object[] row)
        {
            RawResult.Add(row);
        }

        public void LoadFromDataReader(IDataReader reader)
        {
            try
            {
                while (reader.Read())
                {
                    object[] row = new object[ColumnsCount];
                    reader.GetValues(row);
                    RawResult.Add(row);
                }
            }
            finally
            {
                try
                {
                    reader.Close();
                }
                catch (Exception)
                {
                }
            }
        }

        public bool Any()
        {
            return RawResult.Count > 0;
        }
    }
}
