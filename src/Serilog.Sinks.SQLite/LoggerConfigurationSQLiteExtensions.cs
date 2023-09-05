#region

using System;
using System.IO;
using System.Reflection;

using Serilog.Configuration;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.SQLite.Sinks.SQLite;

#endregion

namespace Serilog.Sinks.SQLite
{
    /// <summary>
    ///     Adds the WriteTo.SQLite() extension method to <see cref="LoggerConfiguration" />.
    /// </summary>
    public static class LoggerConfigurationSQLiteExtensions
    {
        /// <summary>
        ///     Adds a sink that writes log events to a SQLite database.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="sqliteDbPath">The path of SQLite db.</param>
        /// <param name="tableName">The name of the SQLite table to store log.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="storeTimestampInUtc">Store timestamp in UTC format</param>
        /// <param name="retentionPeriod">
        ///     The maximum time that a log entry will be kept in the database, or null to disable
        ///     automatic deletion of old log entries. Non-null values smaller than 30 minute will be replaced with 30 minute.
        /// </param>
        /// <param name="retentionCheckInterval">Time period to execute TTL process. Time span should be in 15 minutes increment</param>
        /// <param name="batchSize">Number of messages to save as batch to database. Default is 10, max 1000</param>
        /// <param name="levelSwitch">
        ///     A switch allowing the pass-through minimum level to be changed at runtime.
        /// </param>
        /// <param name="maxDatabaseSize">Maximum database file size can grow in MB. Default 10 MB, maximum 20 GB</param>
        /// <param name="rollOver">If file size grows past max database size, creating rolling backup</param>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        public static LoggerConfiguration SQLite(this LoggerSinkConfiguration loggerConfiguration, string sqliteDbPath,
            string tableName = "Logs", LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum, IFormatProvider formatProvider = null,
            bool storeTimestampInUtc = false, TimeSpan? retentionPeriod = null, TimeSpan? retentionCheckInterval = null,
            LoggingLevelSwitch levelSwitch = null, uint batchSize = 100, uint maxDatabaseSize = 10, bool rollOver = true)
        {
            if (loggerConfiguration == null)
            {
                SelfLog.WriteLine("Logger configuration is null");

                throw new ArgumentNullException(nameof(loggerConfiguration));
            }

            if (string.IsNullOrEmpty(sqliteDbPath))
            {
                SelfLog.WriteLine("Invalid sqliteDbPath");

                throw new ArgumentNullException(nameof(sqliteDbPath));
            }

            if (!Uri.TryCreate(sqliteDbPath, UriKind.RelativeOrAbsolute, out Uri sqliteDbPathUri))
            {
                throw new ArgumentException($"Invalid path {nameof(sqliteDbPath)}");
            }

            if (!sqliteDbPathUri.IsAbsoluteUri)
            {
                string basePath = Assembly.GetEntryAssembly().Location;
                sqliteDbPath = Path.Combine(Path.GetDirectoryName(basePath) ?? throw new NullReferenceException(), sqliteDbPath);
            }

            try
            {
                FileInfo sqliteDbFile = new FileInfo(sqliteDbPath);
                sqliteDbFile.Directory?.Create();

                return loggerConfiguration.Sink(
                    new SQLiteSink(
                        sqliteDbFile.FullName,
                        tableName,
                        formatProvider,
                        storeTimestampInUtc,
                        retentionPeriod,
                        retentionCheckInterval,
                        batchSize,
                        maxDatabaseSize,
                        rollOver
                    ),
                    restrictedToMinimumLevel,
                    levelSwitch
                );
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);

                throw;
            }
        }
    }
}