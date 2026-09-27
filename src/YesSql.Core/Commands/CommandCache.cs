using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace YesSql.Commands
{
    /// <summary>
    /// A command that can execute using the commands cached by a <see cref="CommandCache"/>.
    /// </summary>
    internal interface IReusableCommand
    {
        Task ExecuteAsync(DbConnection connection, DbTransaction transaction, ISqlDialect dialect, ILogger logger, CommandCache cache, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Holds the <see cref="DbCommand"/> instances created while a session is flushed, so that the commands
    /// executing the same statement reuse the same <see cref="DbCommand"/> and only update its parameter values.
    /// Providers that prepare statements for each new command (e.g. SQLite) then prepare each statement only once.
    /// </summary>
    internal sealed class CommandCache : IAsyncDisposable
    {
        // Strings longer than this are sent with an unlimited size, like Dapper does.
        private const int DefaultStringLength = 4000;

        private readonly DbConnection _connection;
        private readonly DbTransaction _transaction;

        // The statements are the cached SQL strings of the commands, so they are compared by reference. This is also
        // safer than comparing their text, as a statement is only reused for the command type that created it (e.g. two
        // index types with the same table name have distinct statements, and distinct parameters).
        private Dictionary<string, DbCommand> _commands;

        public CommandCache(DbConnection connection, DbTransaction transaction)
        {
            _connection = connection;
            _transaction = transaction;
        }

        /// <summary>
        /// Returns the command for the specified statement, and whether it was just created (and needs its parameters to be added).
        /// </summary>
        public DbCommand GetCommand(string sql, out bool created)
        {
            _commands ??= new Dictionary<string, DbCommand>(ReferenceEqualityComparer.Instance);

            if (_commands.TryGetValue(sql, out var command))
            {
                created = false;
                return command;
            }

            command = _connection.CreateCommand();
            command.Transaction = _transaction;
            command.CommandText = sql;
            _commands.Add(sql, command);

            created = true;
            return command;
        }

        /// <summary>
        /// Adds parameters with the specified names to a command.
        /// </summary>
        public static void AddParameters(DbCommand command, params ReadOnlySpan<string> names)
        {
            foreach (var name in names)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = name;
                command.Parameters.Add(parameter);
            }
        }

        public static void SetValue(DbParameter parameter, long value)
        {
            parameter.DbType = DbType.Int64;
            parameter.Value = value;
        }

        public static void SetValue(DbParameter parameter, string value)
        {
            parameter.DbType = DbType.String;

            if (value is null)
            {
                parameter.Value = DBNull.Value;
                parameter.Size = 0;
            }
            else
            {
                parameter.Value = value;
                parameter.Size = value.Length > DefaultStringLength ? -1 : DefaultStringLength;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_commands is null)
            {
                return;
            }

            foreach (var command in _commands.Values)
            {
                await command.DisposeAsync();
            }

            _commands = null;
        }
    }
}
