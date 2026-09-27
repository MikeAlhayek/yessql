using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace YesSql.Tests
{
    /// <summary>
    /// Wraps the connections of a factory to count how many times their commands are cancelled.
    /// </summary>
    public sealed class CancelCountingConnectionFactory : IConnectionFactory
    {
        private readonly IConnectionFactory _factory;
        private int _cancellations;

        public CancelCountingConnectionFactory(IConnectionFactory factory)
        {
            _factory = factory;
        }

        public int Cancellations => _cancellations;

        public Type DbConnectionType => _factory.DbConnectionType;

        public DbConnection CreateConnection() => new CountingConnection(_factory.CreateConnection(), this);

        private void OnCancel() => Interlocked.Increment(ref _cancellations);

        private sealed class CountingConnection : DbConnection
        {
            private readonly DbConnection _inner;
            private readonly CancelCountingConnectionFactory _factory;

            public CountingConnection(DbConnection inner, CancelCountingConnectionFactory factory)
            {
                _inner = inner;
                _factory = factory;
            }

            [AllowNull]
            public override string ConnectionString { get => _inner.ConnectionString; set => _inner.ConnectionString = value; }
            public override string Database => _inner.Database;
            public override string DataSource => _inner.DataSource;
            public override string ServerVersion => _inner.ServerVersion;
            public override ConnectionState State => _inner.State;
            public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
            public override void Close() => _inner.Close();
            public override Task CloseAsync() => _inner.CloseAsync();
            public override void Open() => _inner.Open();
            public override Task OpenAsync(CancellationToken cancellationToken) => _inner.OpenAsync(cancellationToken);
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => _inner.BeginTransaction(isolationLevel);
            protected override ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken) => _inner.BeginTransactionAsync(isolationLevel, cancellationToken);
            protected override DbCommand CreateDbCommand() => new CountingCommand(_inner.CreateCommand(), this, _factory);

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }

                base.Dispose(disposing);
            }

            public override async ValueTask DisposeAsync()
            {
                await _inner.DisposeAsync();
                await base.DisposeAsync();
            }

            public DbConnection Inner => _inner;
        }

        private sealed class CountingCommand : DbCommand
        {
            private readonly DbCommand _inner;
            private readonly CancelCountingConnectionFactory _factory;
            private CountingConnection _connection;

            public CountingCommand(DbCommand inner, CountingConnection connection, CancelCountingConnectionFactory factory)
            {
                _inner = inner;
                _connection = connection;
                _factory = factory;
            }

            [AllowNull]
            public override string CommandText { get => _inner.CommandText; set => _inner.CommandText = value; }
            public override int CommandTimeout { get => _inner.CommandTimeout; set => _inner.CommandTimeout = value; }
            public override CommandType CommandType { get => _inner.CommandType; set => _inner.CommandType = value; }
            public override bool DesignTimeVisible { get => _inner.DesignTimeVisible; set => _inner.DesignTimeVisible = value; }
            public override UpdateRowSource UpdatedRowSource { get => _inner.UpdatedRowSource; set => _inner.UpdatedRowSource = value; }

            protected override DbConnection DbConnection
            {
                get => _connection;
                set
                {
                    _connection = (CountingConnection)value;
                    _inner.Connection = _connection?.Inner;
                }
            }

            protected override DbParameterCollection DbParameterCollection => _inner.Parameters;
            protected override DbTransaction DbTransaction { get => _inner.Transaction; set => _inner.Transaction = value; }

            public override void Cancel()
            {
                _factory.OnCancel();
                _inner.Cancel();
            }

            protected override DbParameter CreateDbParameter() => _inner.CreateParameter();
            public override int ExecuteNonQuery() => _inner.ExecuteNonQuery();
            public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) => _inner.ExecuteNonQueryAsync(cancellationToken);
            public override object ExecuteScalar() => _inner.ExecuteScalar();
            public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken) => _inner.ExecuteScalarAsync(cancellationToken);
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => _inner.ExecuteReader(behavior);
            protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) => _inner.ExecuteReaderAsync(behavior, cancellationToken);
            public override void Prepare() => _inner.Prepare();

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }

                base.Dispose(disposing);
            }
        }
    }
}
