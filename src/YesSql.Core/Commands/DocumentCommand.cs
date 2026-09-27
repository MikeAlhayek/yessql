using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace YesSql.Commands
{
    public abstract class DocumentCommand : IIndexCommand, ICollectionName
    {
        protected static readonly PropertyInfo[] AllProperties = new PropertyInfo[]
        {
            typeof(Document).GetProperty("Type")
        };

        protected static readonly PropertyInfo[] AllKeys = new PropertyInfo[]
        {
            typeof(Document).GetProperty("Id")
        };

        private static readonly ConcurrentDictionary<DocumentCommandKey, string> InsertCommands = new();
        private static readonly ConcurrentDictionary<DocumentCommandKey, string> UpdateCommands = new();
        private static readonly ConcurrentDictionary<DocumentCommandKey, string> UnversionedUpdateCommands = new();
        private static readonly ConcurrentDictionary<DocumentCommandKey, string> DeleteCommands = new();
        private static readonly ConcurrentDictionary<DocumentCommandKey, BatchParts> BatchInsertParts = new();
        private static readonly ConcurrentDictionary<DocumentCommandKey, BatchParts> BatchUpdateParts = new();
        private static readonly ConcurrentDictionary<DocumentCommandKey, BatchParts> BatchDeleteParts = new();

        public abstract int ExecutionOrder { get; }

        protected DocumentCommand(Document document, string collection)
        {
            Document = document;
            Collection = collection;
        }

        public Document Document { get; }

        public string Collection { get; }

        public abstract Task ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger, CancellationToken cancellationToken = default);

        public abstract bool AddToBatch(ISqlDialect dialect, List<string> queries, DbCommand parameters, List<Action<DbDataReader>> actions, int index);

        /// <summary>
        /// Clears the cached document SQL statements. Invoked when a store is (re)initialized.
        /// </summary>
        public static void ResetQueryCache()
        {
            InsertCommands.Clear();
            UpdateCommands.Clear();
            UnversionedUpdateCommands.Clear();
            DeleteCommands.Clear();
            BatchInsertParts.Clear();
            BatchUpdateParts.Clear();
            BatchDeleteParts.Clear();
        }

        /// <summary>
        /// Returns the cached <c>insert</c> statement for a document table. The statement only depends on
        /// the dialect, schema, table prefix and collection, so it can be reused across commands.
        /// </summary>
        protected static string GetInsertCommandText(ISqlDialect dialect, IStore store, string collection)
        {
            var key = new DocumentCommandKey(dialect.Name, store.Configuration.Schema, store.Configuration.TablePrefix, collection);

            if (!InsertCommands.TryGetValue(key, out var result))
            {
                var documentTable = store.Configuration.TableNameConvention.GetDocumentTable(collection);

                InsertCommands[key] = result = $"insert into {dialect.QuoteForTableName(store.Configuration.TablePrefix + documentTable, store.Configuration.Schema)} ({dialect.QuoteForColumnName("Id")}, {dialect.QuoteForColumnName("Type")}, {dialect.QuoteForColumnName("Content")}, {dialect.QuoteForColumnName("Version")}) values (@Id, @Type, @Content, @Version);";
            }

            return result;
        }

        /// <summary>
        /// Returns the cached <c>update</c> statement (without the optional version check clause) for a document table.
        /// </summary>
        protected static string GetUpdateCommandText(ISqlDialect dialect, IStore store, string collection)
        {
            var key = new DocumentCommandKey(dialect.Name, store.Configuration.Schema, store.Configuration.TablePrefix, collection);

            if (!UpdateCommands.TryGetValue(key, out var result))
            {
                var documentTable = store.Configuration.TableNameConvention.GetDocumentTable(collection);

                UpdateCommands[key] = result = $"update {dialect.QuoteForTableName(store.Configuration.TablePrefix + documentTable, store.Configuration.Schema)} "
                    + $"set {dialect.QuoteForColumnName("Content")} = @Content, {dialect.QuoteForColumnName("Version")} = @Version where "
                    + $"{dialect.QuoteForColumnName("Id")} = @Id ";
            }

            return result;
        }

        /// <summary>
        /// Returns the cached, terminated <c>update</c> statement used when no version check is required.
        /// </summary>
        protected static string GetUnversionedUpdateCommandText(ISqlDialect dialect, IStore store, string collection)
        {
            var key = new DocumentCommandKey(dialect.Name, store.Configuration.Schema, store.Configuration.TablePrefix, collection);

            if (!UnversionedUpdateCommands.TryGetValue(key, out var result))
            {
                UnversionedUpdateCommands[key] = result = GetUpdateCommandText(dialect, store, collection) + ";";
            }

            return result;
        }

        /// <summary>
        /// Returns the cached <c>delete</c> statement for a document table.
        /// </summary>
        protected static string GetDeleteCommandText(ISqlDialect dialect, IStore store, string collection)
        {
            var key = new DocumentCommandKey(dialect.Name, store.Configuration.Schema, store.Configuration.TablePrefix, collection);

            if (!DeleteCommands.TryGetValue(key, out var result))
            {
                var documentTable = store.Configuration.TableNameConvention.GetDocumentTable(collection);

                DeleteCommands[key] = result = $"delete from {dialect.QuoteForTableName(store.Configuration.TablePrefix + documentTable, store.Configuration.Schema)} where {dialect.QuoteForColumnName("Id")} = @Id;";
            }

            return result;
        }

        /// <summary>
        /// Returns the batched <c>insert</c> statement of a document, for the parameters with the specified suffix.
        /// </summary>
        protected static string GetBatchInsertCommandText(ISqlDialect dialect, IStore store, string collection, string suffix)
        {
            var key = new DocumentCommandKey(dialect.Name, store.Configuration.Schema, store.Configuration.TablePrefix, collection);

            if (!BatchInsertParts.TryGetValue(key, out var parts))
            {
                var documentTable = store.Configuration.TableNameConvention.GetDocumentTable(collection);

                BatchInsertParts[key] = parts = new BatchParts(
                    $"insert into {dialect.QuoteForTableName(store.Configuration.TablePrefix + documentTable, store.Configuration.Schema)} ({dialect.QuoteForColumnName("Id")}, {dialect.QuoteForColumnName("Type")}, {dialect.QuoteForColumnName("Content")}, {dialect.QuoteForColumnName("Version")}) values (@Id_",
                    ", @Type_",
                    ", @Content_",
                    ", @Version_",
                    ");");
            }

            // A single string is allocated per statement. With C# 13+ on .NET 9+ this call binds to the
            // string.Concat(params ReadOnlySpan<string>) overload, so no intermediate array is allocated.
            return string.Concat(parts.P0, suffix, parts.P1, suffix, parts.P2, suffix, parts.P3, suffix, parts.P4);
        }

        /// <summary>
        /// Returns the batched <c>update</c> statement of a document (without version check), for the parameters with the specified suffix.
        /// </summary>
        protected static string GetBatchUpdateCommandText(ISqlDialect dialect, IStore store, string collection, string suffix)
        {
            var key = new DocumentCommandKey(dialect.Name, store.Configuration.Schema, store.Configuration.TablePrefix, collection);

            if (!BatchUpdateParts.TryGetValue(key, out var parts))
            {
                var documentTable = store.Configuration.TableNameConvention.GetDocumentTable(collection);

                BatchUpdateParts[key] = parts = new BatchParts(
                    $"update {dialect.QuoteForTableName(store.Configuration.TablePrefix + documentTable, store.Configuration.Schema)} set {dialect.QuoteForColumnName("Content")} = @Content_",
                    $", {dialect.QuoteForColumnName("Version")} = @Version_",
                    $" where {dialect.QuoteForColumnName("Id")} = @Id_",
                    ";",
                    null);
            }

            return string.Concat(parts.P0, suffix, parts.P1, suffix, parts.P2, suffix, parts.P3);
        }

        /// <summary>
        /// Returns the batched <c>delete</c> statement of a document, for the parameters with the specified suffix.
        /// </summary>
        protected static string GetBatchDeleteCommandText(ISqlDialect dialect, IStore store, string collection, string suffix)
        {
            var key = new DocumentCommandKey(dialect.Name, store.Configuration.Schema, store.Configuration.TablePrefix, collection);

            if (!BatchDeleteParts.TryGetValue(key, out var parts))
            {
                var documentTable = store.Configuration.TableNameConvention.GetDocumentTable(collection);

                BatchDeleteParts[key] = parts = new BatchParts(
                    $"delete from {dialect.QuoteForTableName(store.Configuration.TablePrefix + documentTable, store.Configuration.Schema)} where {dialect.QuoteForColumnName("Id")} = @Id_",
                    ";",
                    null,
                    null,
                    null);
            }

            return string.Concat(parts.P0, suffix, parts.P1);
        }

        private sealed record BatchParts(string P0, string P1, string P2, string P3, string P4);

        private readonly record struct DocumentCommandKey(string Dialect, string Schema, string Prefix, string Collection);
    }
}
