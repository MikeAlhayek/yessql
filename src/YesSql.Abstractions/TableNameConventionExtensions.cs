using System;
using System.Linq;

namespace YesSql;
public static class TableNameConventionExtensions
{
    public static string GetTableName(this ITableNameConvention tableNameConvention, Type type, params string[] names)
    {
        return tableNameConvention.GetTableName(new[] { type.Name }.Concat(names).ToArray());
    }
}
