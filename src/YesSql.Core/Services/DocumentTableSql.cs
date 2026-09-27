namespace YesSql.Services
{
    /// <summary>
    /// The SQL fragments referencing the document table of a collection. They only depend on the store configuration,
    /// so they are computed once per collection instead of for each query.
    /// </summary>
    internal sealed class DocumentTableSql
    {
        public DocumentTableSql(ISqlBuilder builder, ISqlDialect dialect, string documentTable, string schema)
        {
            Table = builder.FormatTable(documentTable, schema);
            IdColumn = builder.FormatColumn(documentTable, "Id", schema);
            AllColumns = builder.FormatColumn(documentTable, "*", schema);
            TypeFilter = builder.FormatColumn(documentTable, "Type", schema) + " = @Type";
            CountDistinctId = "count(distinct " + IdColumn + ")";
            IndexQueryAlias = dialect.QuoteForAliasName("IndexQuery");
            IndexQueryId = builder.FormatColumn("IndexQuery", "Id", schema, true);
        }

        /// <summary>The document table, e.g. <c>[Document]</c>.</summary>
        public string Table { get; }

        /// <summary>The document identifier column, e.g. <c>[Document].[Id]</c>.</summary>
        public string IdColumn { get; }

        /// <summary>All the document columns, e.g. <c>[Document].*</c>.</summary>
        public string AllColumns { get; }

        /// <summary>The filter on the document type, e.g. <c>[Document].[Type] = @Type</c>.</summary>
        public string TypeFilter { get; }

        /// <summary>The count of distinct documents, e.g. <c>count(distinct [Document].[Id])</c>.</summary>
        public string CountDistinctId { get; }

        /// <summary>The alias of the de-duplication sub-query.</summary>
        public string IndexQueryAlias { get; }

        /// <summary>The identifier column of the de-duplication sub-query.</summary>
        public string IndexQueryId { get; }
    }
}
