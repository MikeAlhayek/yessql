using Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace YesSql.Commands
{
    public sealed class DeleteDocumentCommand : DocumentCommand, IReusableCommand
    {
        private readonly IStore _store;
        public override int ExecutionOrder { get; } = 4;

        public DeleteDocumentCommand(Document document, IStore store, string collection) : base(document, collection)
        {
            _store = store;
        }

        public override Task ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger, CancellationToken cancellationToken = default)
        {
            var deleteCmd = GetDeleteCommandText(dialect, _store, Collection);

            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace(deleteCmd);
            }
            
            return connection.ExecuteAsync(new CommandDefinition(deleteCmd, Document, transaction, null, null, CommandFlags.Buffered, cancellationToken));
        }

        Task IReusableCommand.ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger, CommandCache cache, CancellationToken cancellationToken)
        {
            var deleteCmd = GetDeleteCommandText(dialect, _store, Collection);

            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace(deleteCmd);
            }

            var command = cache.GetCommand(deleteCmd, out var created);

            if (created)
            {
                CommandCache.AddParameters(command, "Id");
            }

            CommandCache.SetValue(command.Parameters[0], Document.Id);

            return command.ExecuteNonQueryAsync(cancellationToken);
        }

        public override bool AddToBatch(ISqlDialect dialect, List<string> queries, DbCommand command, List<Action<DbDataReader>> actions, int index)
        {
            var suffix = index.ToString(CultureInfo.InvariantCulture);

            queries.Add(GetBatchDeleteCommandText(dialect, _store, Collection, suffix));
            command.AddParameter("Id_" + suffix, Document.Id);

            return true;
        }
    }
}
