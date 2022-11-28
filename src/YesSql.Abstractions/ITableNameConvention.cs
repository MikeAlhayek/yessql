using System;

namespace YesSql
{
    /// <summary>
    /// A class implementing this interface can customize how table names are generated.
    /// </summary>
    public interface ITableNameConvention
    {
        /// <summary>
        /// Returns the name of a Document table. 
        /// </summary>
        string GetDocumentTable(string collection = null);

        /// <summary>
        /// Returns the name of an Index table. 
        /// </summary>
        string GetIndexTable(Type indexType, string collection = null);

        /// <summary>
        /// Combines the given names into a standard table name.
        /// </summary>
        /// <param name="names"></param>
        /// <returns></returns>
        string GetTableName(params string[] names);
    }
}
