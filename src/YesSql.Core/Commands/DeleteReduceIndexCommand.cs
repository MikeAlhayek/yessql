using Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using YesSql.Indexes;

namespace YesSql.Commands
{
    public sealed class DeleteReduceIndexCommand : IndexCommand
    {
        public DeleteReduceIndexCommand(IIndex index, IStore store, string collection) : base(index, store, collection)
        {
        }

        public override int ExecutionOrder { get; } = 1;

        public override bool AddToBatch(ISqlDialect dialect, List<string> queries, DbCommand batchCommand, List<Action<DbDataReader>> actions, int index)
        {
            var type = Index.GetType();
            var name = type.Name;

            var documentTable = _store.Configuration.TableNameConvention.GetDocumentTable(Collection);
            var indexTableName = _store.Configuration.TableNameConvention.GetIndexTable(type, Collection);

            var bridgeTableName = _store.Configuration.TableNameConvention.GetTableName(indexTableName, documentTable);

            var bridgeSql = $"delete from {dialect.QuoteForTableName(_store.Configuration.TablePrefix + bridgeTableName)} where {dialect.QuoteForColumnName(name + _store.Configuration.NameConventionOptions.IdColumnName)} = @Id_{index};";
            var command = $"delete from {dialect.QuoteForTableName(_store.Configuration.TablePrefix + _store.Configuration.TableNameConvention.GetIndexTable(type, Collection))} where {dialect.QuoteForColumnName(_store.Configuration.NameConventionOptions.IdColumnName)} = @Id_{index};";
            queries.Add(bridgeSql);
            queries.Add(command);
            batchCommand.AddParameter("Id_" + index, Index.Id);

            return true;
        }

        public override async Task ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger)
        {
            var type = Index.GetType();
            var name = type.Name;

            var documentTable = _store.Configuration.TableNameConvention.GetDocumentTable(Collection);
            var indexTableName = _store.Configuration.TableNameConvention.GetIndexTable(type, Collection);

            var bridgeTableName = _store.Configuration.TableNameConvention.GetTableName(indexTableName, documentTable);

            var bridgeSql = "delete from " + dialect.QuoteForTableName(_store.Configuration.TablePrefix + bridgeTableName) + " where " + dialect.QuoteForColumnName(name + _store.Configuration.NameConventionOptions.IdColumnName) + " = @Id;";
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace(bridgeSql);
            }
            await connection.ExecuteAsync(bridgeSql, new { Id = Index.Id }, transaction);
            var command = "delete from " + dialect.QuoteForTableName(_store.Configuration.TablePrefix + _store.Configuration.TableNameConvention.GetIndexTable(type, Collection)) + " where " + dialect.QuoteForColumnName(_store.Configuration.NameConventionOptions.IdColumnName) + " = @Id;";
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace(command);
            }
            await connection.ExecuteAsync(command, new { Id = Index.Id }, transaction);
        }
    }
}
