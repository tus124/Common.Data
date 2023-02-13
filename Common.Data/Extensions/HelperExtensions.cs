using System.Data;
using System.Reflection;

namespace Common.Data;

public static class HelperExtensions
{
    public static DataTable ToDataTable<T>(this IEnumerable<T> data)
    {
        DataTable dt = new DataTable(typeof(T).Name);

        PropertyInfo[] pi = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        foreach (PropertyInfo p in pi)
        {
            dt.Columns.Add(p.Name, BaseType(p.PropertyType));
        }

        foreach (T item in data)
        {
            object[] values = new object[pi.Length];

            for (int i = 0; i < pi.Length; i++)
            {
                values[i] = pi[i].GetValue(item, null);
            }
            dt.Rows.Add(values);
        }

        return dt;
    }

    private static Type BaseType(Type type)
    {
        if(type != null && type.IsValueType && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            return Nullable.GetUnderlyingType(type);
        }
        else
        {
            return type;
        }
    }
}
