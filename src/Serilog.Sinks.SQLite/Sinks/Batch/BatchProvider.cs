﻿#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Serilog.Debugging;
using Serilog.Events;

#endregion

namespace Serilog.Sinks.SQLite.Sinks.Batch
{
    internal abstract class BatchProvider : IDisposable
    {
        private const    int                                 MaxSupportedBufferSize = 100_000;
        private const    int                                 MaxSupportedBatchSize  = 1_000;
        private readonly BlockingCollection<IList<LogEvent>> _batchEventsCollection;
        private readonly int                                 _batchSize;
        private readonly Task                                _batchTask;
        private readonly CancellationTokenSource             _cancellationTokenSource = new CancellationTokenSource();
        private readonly Task                                _eventPumpTask;
        private readonly BlockingCollection<LogEvent>        _eventsCollection;
        private readonly ConcurrentQueue<LogEvent>           _logEventBatch;
        private readonly int                                 _maxBufferSize;
        private readonly SemaphoreSlim                       _semaphoreSlim   = new SemaphoreSlim(1, 1);
        private readonly AutoResetEvent                      _timerResetEvent = new AutoResetEvent(false);
        private readonly Task                                _timerTask;
        private readonly TimeSpan                            _timerThresholdSpan     = TimeSpan.FromSeconds(10);
        private readonly TimeSpan                            _transientThresholdSpan = TimeSpan.FromSeconds(5);
        private          bool                                _canStop;
        private          int                                 _numMessages;

        protected BatchProvider(int batchSize = 100, int maxBufferSize = 25_000)
        {
            _maxBufferSize = Math.Min(Math.Max(5_000,     maxBufferSize), MaxSupportedBufferSize);
            _batchSize     = Math.Min(Math.Max(batchSize, 1),             MaxSupportedBatchSize);

            _logEventBatch         = new ConcurrentQueue<LogEvent>();
            _batchEventsCollection = new BlockingCollection<IList<LogEvent>>();
            _eventsCollection      = new BlockingCollection<LogEvent>(maxBufferSize);

            _batchTask     = Task.Factory.StartNew(PumpAsync, TaskCreationOptions.LongRunning);
            _timerTask     = Task.Factory.StartNew(TimerPump, TaskCreationOptions.LongRunning);
            _eventPumpTask = Task.Factory.StartNew(EventPump, TaskCreationOptions.LongRunning);
        }

        private async Task PumpAsync()
        {
            try
            {
                while (!_batchEventsCollection.IsCompleted)
                {
                    IList<LogEvent> logEvents = _batchEventsCollection.Take(_cancellationTokenSource.Token);
                    SelfLog.WriteLine($"Sending batch of {logEvents.Count} logs");

                    bool retValue = await WriteLogEventAsync(logEvents).ConfigureAwait(false);

                    if (retValue)
                    {
                        Interlocked.Add(ref _numMessages, -1 * logEvents.Count);
                    }
                    else
                    {
                        SelfLog.WriteLine($"Retrying after {_transientThresholdSpan.TotalSeconds} seconds...");

                        await Task.Delay(_transientThresholdSpan).ConfigureAwait(false);

                        if (!_batchEventsCollection.IsAddingCompleted)
                        {
                            _batchEventsCollection.Add(logEvents);
                        }
                    }

                    if (_cancellationTokenSource.IsCancellationRequested)
                    {
                        _cancellationTokenSource.Token.ThrowIfCancellationRequested();
                    }
                }
            }
            catch (InvalidOperationException) { }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }

        private void TimerPump()
        {
            while (!_canStop)
            {
                _timerResetEvent.WaitOne(_timerThresholdSpan);
                FlushLogEventBatch();
            }
        }

        private void EventPump()
        {
            try
            {
                while (!_eventsCollection.IsCompleted)
                {
                    LogEvent logEvent = _eventsCollection.Take(_cancellationTokenSource.Token);
                    _logEventBatch.Enqueue(logEvent);

                    if (_logEventBatch.Count >= _batchSize)
                    {
                        FlushLogEventBatch();
                    }
                }
            }
            catch (InvalidOperationException) { }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }

        private void FlushLogEventBatch()
        {
            try
            {
                _semaphoreSlim.Wait(_cancellationTokenSource.Token);

                if (!_logEventBatch.Any())
                {
                    return;
                }

                int logEventBatchSize = _logEventBatch.Count >= _batchSize
                    ? _batchSize
                    : _logEventBatch.Count;
                List<LogEvent> logEventList = new List<LogEvent>();

                for (int i = 0; i < logEventBatchSize; i++)
                {
                    if (_logEventBatch.TryDequeue(out LogEvent logEvent))
                    {
                        logEventList.Add(logEvent);
                    }
                }

                if (!_batchEventsCollection.IsAddingCompleted)
                {
                    _batchEventsCollection.Add(logEventList);
                }
            }
            catch (InvalidOperationException) { }
            catch (OperationCanceledException) { }
            finally
            {
                if (!_cancellationTokenSource.IsCancellationRequested)
                {
                    _semaphoreSlim.Release();
                }
            }
        }

        protected void PushEvent(LogEvent logEvent)
        {
            if (_numMessages > _maxBufferSize)
            {
                return;
            }

            if (_eventsCollection.IsAddingCompleted)
            {
                return;
            }

            _eventsCollection.Add(logEvent);
            Interlocked.Increment(ref _numMessages);
        }

        protected abstract Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch);

        #region IDisposable Support

        private bool _disposedValue; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (_disposedValue)
            {
                return;
            }

            if (disposing)
            {
                FlushAndCloseEventHandlers();
                _semaphoreSlim.Dispose();

                SelfLog.WriteLine("Sink halted successfully.");
            }

            _disposedValue = true;
        }

        private void FlushAndCloseEventHandlers()
        {
            try
            {
                SelfLog.WriteLine("Halting sink...");

                _canStop = true;
                _timerResetEvent.Set();
                _eventsCollection.CompleteAdding();

                // Flush events collection
                while (!_eventsCollection.IsCompleted)
                {
                    LogEvent logEvent = _eventsCollection.Take();
                    _logEventBatch.Enqueue(logEvent);

                    if (_logEventBatch.Count >= _batchSize)
                    {
                        FlushLogEventBatch();
                    }
                }

                FlushLogEventBatch();

                _batchEventsCollection.CompleteAdding();

                // request cancellation of all tasks
                _cancellationTokenSource.Cancel();

                // Flush events batch
                while (!_batchEventsCollection.IsCompleted)
                {
                    IList<LogEvent> eventBatch = _batchEventsCollection.Take();
                    WriteLogEventAsync(eventBatch).GetAwaiter().GetResult();
                    SelfLog.WriteLine($"Sending batch of {eventBatch.Count} logs");
                }

                Task.WaitAll(new[] {_eventPumpTask, _batchTask, _timerTask}, TimeSpan.FromSeconds(60));
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}