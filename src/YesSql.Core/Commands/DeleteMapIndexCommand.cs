using Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace YesSql.Commands
{
    public sealed class DeleteMapIndexCommand : IIndexCommand, ICollectionName, IReusableCommand
    {
        private static readonly ConcurrentDictionary<CommandKey, string> DeleteCommands = new();
        private static readonly ConcurrentDictionary<CommandKey, string> BatchDeleteCommandPrefixes = new();

        private readonly IStore _store;
        public long DocumentId { get; }
        public Type IndexType { get; }
        public string Collection { get; }
        public int ExecutionOrder { get; } = 1;

        public DeleteMapIndexCommand(Type indexType, long documentId, IStore store, string collection)
        {
            IndexType = indexType;
            DocumentId = documentId;
            Collection = collection;
            _store = store;
        }

        public Task ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger, CancellationToken cancellationToken = default)
        {
            var command = GetDeleteCommandText(dialect);


            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace(command);
            }

            return connection.ExecuteAsync(new CommandDefinition(command, new { Id = DocumentId }, transaction, null, null, CommandFlags.Buffered, cancellationToken));
        }

        Task IReusableCommand.ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger, CommandCache cache, CancellationToken cancellationToken)
        {
            var sql = GetDeleteCommandText(dialect);

            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace(sql);
            }

            var command = cache.GetCommand(sql, out var created);

            if (created)
            {
                CommandCache.AddParameters(command, "Id");
            }

            CommandCache.SetValue(command.Parameters[0], DocumentId);

            return command.ExecuteNonQueryAsync(cancellationToken);
        }

        /// <summary>
        /// Clears the cached SQL statements. Invoked when a store is (re)initialized.
        /// </summary>
        public static void ResetQueryCache()
        {
            DeleteCommands.Clear();
            BatchDeleteCommandPrefixes.Clear();
        }

        private string GetDeleteCommandText(ISqlDialect dialect)
        {
            var key = new CommandKey(dialect.Name, IndexType, _store.Configuration.Schema, _store.Configuration.TablePrefix, Collection);

            if (!DeleteCommands.TryGetValue(key, out var result))
            {
                DeleteCommands[key] = result = $"delete from {dialect.QuoteForTableName(_store.Configuration.TablePrefix + _store.Configuration.TableNameConvention.GetIndexTable(IndexType, Collection), _store.Configuration.Schema)} where {dialect.QuoteForColumnName("DocumentId")} = @Id;";
            }

            return result;
        }

        public bool AddToBatch(ISqlDialect dialect, List<string> queries, DbCommand command, List<Action<DbDataReader>> actions, int index)
        {
            var key = new CommandKey(dialect.Name, IndexType, _store.Configuration.Schema, _store.Configuration.TablePrefix, Collection);

            if (!BatchDeleteCommandPrefixes.TryGetValue(key, out var prefix))
            {
                BatchDeleteCommandPrefixes[key] = prefix = $"delete from {dialect.QuoteForTableName(_store.Configuration.TablePrefix + _store.Configuration.TableNameConvention.GetIndexTable(IndexType, Collection), _store.Configuration.Schema)} where {dialect.QuoteForColumnName("DocumentId")} = @Id_";
            }

            var suffix = index.ToString(CultureInfo.InvariantCulture);

            queries.Add(string.Concat(prefix, suffix, ";"));

            command.AddParameter("Id_" + suffix, DocumentId);

            return true;
        }

        private readonly record struct CommandKey(string Dialect, Type IndexType, string Schema, string Prefix, string Collection);
    }
}
