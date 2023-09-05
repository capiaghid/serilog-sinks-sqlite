#region

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.SQLite.Sinks.Batch;
using Serilog.Sinks.SQLite.Sinks.Extensions;

// ReSharper disable InconsistentNaming
// ReSharper disable NotAccessedField.Local

#endregion

namespace Serilog.Sinks.SQLite.Sinks.SQLite
{
    internal class SQLiteSink : BatchProvider, ILogEventSink
    {
        private const           string          TimestampFormat          = "yyyy-MM-ddTHH:mm:ss.fff";
        private const           long            BytesPerMb               = 1_048_576;
        private const           long            MaxSupportedPages        = 5_242_880;
        private const           long            MaxSupportedPageSize     = 4096;
        private const           long            MaxSupportedDatabaseSize = unchecked(MaxSupportedPageSize * MaxSupportedPages) / 1048576;
        private readonly static SemaphoreSlim   semaphoreSlim            = new SemaphoreSlim(1, 1);
        private readonly        string          _databasePath;
        private readonly        IFormatProvider _formatProvider;
        private readonly        uint            _maxDatabaseSize;
        private readonly        TimeSpan?       _retentionPeriod;
        private readonly        Timer           _retentionTimer;
        private readonly        bool            _rollOver;
        private readonly        bool            _storeTimestampInUtc;
        private readonly        string          _tableName;

        public SQLiteSink(string sqlLiteDbPath, string tableName, IFormatProvider formatProvider, bool storeTimestampInUtc,
            TimeSpan? retentionPeriod, TimeSpan? retentionCheckInterval, uint batchSize = 100, uint maxDatabaseSize = 10,
            bool rollOver = true) : base((int)batchSize, 100_000)
        {
            _databasePath        = sqlLiteDbPath;
            _tableName           = tableName;
            _formatProvider      = formatProvider;
            _storeTimestampInUtc = storeTimestampInUtc;
            _maxDatabaseSize     = maxDatabaseSize;
            _rollOver            = rollOver;

            if (maxDatabaseSize > MaxSupportedDatabaseSize)
            {
                throw new SQLiteException($"Database size greater than {MaxSupportedDatabaseSize} MB is not supported");
            }

            InitializeDatabase();

            if (retentionPeriod.HasValue)
            {
                // impose a min retention period of 15 minute
                int retentionCheckMinutes = 15;

                if (retentionCheckInterval.HasValue)
                {
                    retentionCheckMinutes = Math.Max(retentionCheckMinutes, retentionCheckInterval.Value.Minutes);
                }

                // impose multiple of 15 minute interval
                retentionCheckMinutes = retentionCheckMinutes / 15 * 15;

                _retentionPeriod = new[] {retentionPeriod, TimeSpan.FromMinutes(30)}.Max();

                // check for retention at this interval - or use retentionPeriod if not specified
                _retentionTimer = new Timer(
                    x => { ApplyRetentionPolicy(); },
                    null,
                    TimeSpan.FromMinutes(0),
                    TimeSpan.FromMinutes(retentionCheckMinutes)
                );
            }
        }

        #region ILogEvent implementation

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion

        private void InitializeDatabase()
        {
            using (SQLiteConnection conn = GetSqLiteConnection())
            {
                CreateSqlTable(conn);
            }
        }

        private SQLiteConnection GetSqLiteConnection()
        {
            string sqlConString = new SQLiteConnectionStringBuilder
            {
                DataSource   = _databasePath,
                JournalMode  = SQLiteJournalModeEnum.Memory,
                SyncMode     = SynchronizationModes.Normal,
                CacheSize    = 500,
                PageSize     = (int)MaxSupportedPageSize,
                MaxPageCount = (int)(_maxDatabaseSize * BytesPerMb / MaxSupportedPageSize)
            }.ConnectionString;

            SQLiteConnection sqLiteConnection = new SQLiteConnection(sqlConString);
            sqLiteConnection.Open();

            return sqLiteConnection;
        }

        private void CreateSqlTable(SQLiteConnection sqlConnection)
        {
            string colDefs = "id INTEGER PRIMARY KEY AUTOINCREMENT,";
            colDefs += "Timestamp TEXT,";
            colDefs += "Level VARCHAR(10),";
            colDefs += "Exception TEXT,";
            colDefs += "RenderedMessage TEXT,";
            colDefs += "Properties TEXT";

            string sqlCreateText = $"CREATE TABLE IF NOT EXISTS {_tableName} ({colDefs})";

            SQLiteCommand sqlCommand = new SQLiteCommand(sqlCreateText, sqlConnection);
            sqlCommand.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlInsertCommand(SQLiteConnection connection)
        {
            string sqlInsertText = "INSERT INTO {0} (Timestamp, Level, Exception, RenderedMessage, Properties)";
            sqlInsertText += " VALUES (@timeStamp, @level, @exception, @renderedMessage, @properties)";
            sqlInsertText =  string.Format(sqlInsertText, _tableName);

            SQLiteCommand sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = sqlInsertText;
            sqlCommand.CommandType = CommandType.Text;

            sqlCommand.Parameters.Add(new SQLiteParameter("@timeStamp",       DbType.DateTime2));
            sqlCommand.Parameters.Add(new SQLiteParameter("@level",           DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@exception",       DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@renderedMessage", DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@properties",      DbType.String));

            return sqlCommand;
        }

        private void ApplyRetentionPolicy()
        {
            if (_retentionPeriod == null)
            {
                throw new ArgumentNullException(nameof(_retentionPeriod.Value));
            }

            DateTimeOffset epoch = DateTimeOffset.Now.Subtract(_retentionPeriod.Value);

            using (SQLiteConnection sqlConnection = GetSqLiteConnection())
            {
                using (SQLiteCommand cmd = CreateSqlDeleteCommand(sqlConnection, epoch))
                {
                    SelfLog.WriteLine("Deleting log entries older than {0}", epoch);
                    int ret = cmd.ExecuteNonQuery();
                    SelfLog.WriteLine($"{ret} records deleted");
                }
            }
        }

        private void TruncateLog(SQLiteConnection sqlConnection)
        {
            SQLiteCommand cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName}";
            cmd.ExecuteNonQuery();

            VacuumDatabase(sqlConnection);
        }

        private void VacuumDatabase(SQLiteConnection sqlConnection)
        {
            SQLiteCommand cmd = sqlConnection.CreateCommand();
            cmd.CommandText = "vacuum";
            cmd.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlDeleteCommand(SQLiteConnection sqlConnection, DateTimeOffset epoch)
        {
            SQLiteCommand cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName} WHERE Timestamp < @epoch";

            cmd.Parameters.Add(
                new SQLiteParameter("@epoch", DbType.DateTime2)
                {
                    Value = (_storeTimestampInUtc
                        ? epoch.ToUniversalTime()
                        : epoch).ToString(TimestampFormat)
                }
            );

            return cmd;
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if (logEventsBatch == null || logEventsBatch.Count == 0)
            {
                return true;
            }

            await semaphoreSlim.WaitAsync().ConfigureAwait(false);

            try
            {
                using (SQLiteConnection sqlConnection = GetSqLiteConnection())
                {
                    try
                    {
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);

                        return true;
                    }
                    catch (SQLiteException e)
                    {
                        SelfLog.WriteLine(e.Message);

                        if (e.ResultCode != SQLiteErrorCode.Full)
                        {
                            return false;
                        }

                        if (_rollOver == false)
                        {
                            SelfLog.WriteLine("Discarding log excessive of max database");

                            return true;
                        }

                        string dbExtension = Path.GetExtension(_databasePath);

                        string newFilePath = Path.Combine(
                            Path.GetDirectoryName(_databasePath) ?? "Logs",
                            $"{Path.GetFileNameWithoutExtension(_databasePath)}-{DateTime.Now:yyyyMMdd_HHmmss.ff}{dbExtension}"
                        );

                        File.Copy(_databasePath ?? throw new InvalidOperationException(), newFilePath, true);

                        TruncateLog(sqlConnection);
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);

                        SelfLog.WriteLine($"Rolling database to {newFilePath}");

                        return true;
                    }
                    catch (Exception e)
                    {
                        SelfLog.WriteLine(e.Message);

                        return false;
                    }
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private async Task WriteToDatabaseAsync(ICollection<LogEvent> logEventsBatch, SQLiteConnection sqlConnection)
        {
            using (SQLiteTransaction tr = sqlConnection.BeginTransaction())
            {
                using (SQLiteCommand sqlCommand = CreateSqlInsertCommand(sqlConnection))
                {
                    sqlCommand.Transaction = tr;

                    foreach (LogEvent logEvent in logEventsBatch)
                    {
                        sqlCommand.Parameters["@timeStamp"].Value = _storeTimestampInUtc
                            ? logEvent.Timestamp.ToUniversalTime().ToString(TimestampFormat)
                            : logEvent.Timestamp.ToString(TimestampFormat);
                        sqlCommand.Parameters["@level"].Value     = logEvent.Level.ToString();
                        sqlCommand.Parameters["@exception"].Value = logEvent.Exception?.ToString() ?? string.Empty;

                        sqlCommand.Parameters["@renderedMessage"].Value =
                            logEvent.MessageTemplate.Render(logEvent.Properties, _formatProvider);

                        sqlCommand.Parameters["@properties"].Value = logEvent.Properties.Count > 0
                            ? logEvent.Properties.Json()
                            : string.Empty;

                        await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    tr.Commit();
                }
            }
        }
    }
}