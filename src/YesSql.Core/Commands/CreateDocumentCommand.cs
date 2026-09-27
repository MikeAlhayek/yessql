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
    public sealed class CreateDocumentCommand : DocumentCommand
    {
        private readonly IStore _store;

        public override int ExecutionOrder { get; } = 0;

        public CreateDocumentCommand(Document document, IStore store, string collection) : base(document, collection)
        {
            _store = store;
        }

        public override Task ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger, CancellationToken cancellationToken = default)
        {
            var insertCmd = GetInsertCommandText(dialect, _store, Collection);

            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace(insertCmd);
            }

            return connection.ExecuteAsync(new CommandDefinition(insertCmd, Document, transaction, null, null, CommandFlags.Buffered, cancellationToken));
        }

        public override bool AddToBatch(ISqlDialect dialect, List<string> queries, DbCommand batchCommand, List<Action<DbDataReader>> actions, int index)
        {
            var suffix = index.ToString(CultureInfo.InvariantCulture);

            queries.Add(GetBatchInsertCommandText(dialect, _store, Collection, suffix));

            batchCommand
                .AddParameter("Id_" + suffix, Document.Id)
                .AddParameter("Type_" + suffix, Document.Type)
                .AddParameter("Content_" + suffix, Document.Content)
                .AddParameter("Version_" + suffix, Document.Version);

            return true;
        }
    }
}
