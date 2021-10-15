// --------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// --------------------------------------------------------------------------------------------

using System;
using System.Threading;
using StackExchange.Redis;

namespace ForceReconnect
{
    internal static class RedisForceReconnect
    {
        private static long _lastReconnectTicks = DateTimeOffset.MinValue.UtcTicks;
        private static DateTimeOffset _firstErrorTime = DateTimeOffset.MinValue;
        private static DateTimeOffset _previousErrorTime = DateTimeOffset.MinValue;

        private static SemaphoreSlim _reconnectSemaphore = new SemaphoreSlim(initialCount: 1, maxCount: 1);
        private static SemaphoreSlim _initSemaphore = new SemaphoreSlim(initialCount: 1, maxCount: 1);

        // In general, let StackExchange.Redis handle most reconnects,
        // so limit the frequency of how often this will actually reconnect.
        public static TimeSpan ReconnectMinInterval = TimeSpan.FromSeconds(60);

        // if errors continue for longer than the below threshold, then the
        // multiplexer seems to not be reconnecting, so re-create the multiplexer
        public static TimeSpan ReconnectErrorThreshold = TimeSpan.FromSeconds(30);

        public static TimeSpan RestartConnectionTimeout = TimeSpan.FromSeconds(15);

        private static string _connectionString = "TODO: Call Initialize() method with connection string";
        private static ConnectionMultiplexer connection;
        private static bool _didInitialize = false;

        public static ConnectionMultiplexer Connection { get { return connection; } }

        public static void Initialize(string connectionString)
        {
            if (_didInitialize)
            {
                throw new InvalidOperationException("Cannot initialize more than once.");
            }

            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            _connectionString = connectionString;
            connection = CreateConnection();
            _didInitialize = true;
        }

        /// <summary>
        /// Force a new ConnectionMultiplexer to be created.
        /// NOTES:
        ///     1. Users of the ConnectionMultiplexer MUST handle ObjectDisposedExceptions, which can now happen as a result of calling ForceReconnect().
        ///     2. Call ForceReconnect() for RedisConnectionExceptions and RedisSocketExceptions. You can also call it for RedisTimeoutExceptions,
        ///         but only if you're using generous ReconnectMinInterval and ReconnectErrorThreshold. Otherwise, establishing new connections can cause
        ///         a cascade failure on a server that's timing out because it's already overloaded.
        ///     3. The code will:
        ///         a. wait to reconnect for at least the "ReconnectErrorThreshold" time of repeated errors before actually reconnecting
        ///         b. not reconnect more frequently than configured in "ReconnectMinInterval"
        /// </summary>
        public static void ForceReconnect()
        {
            var utcNow = DateTimeOffset.UtcNow;
            long previousTicks = Interlocked.Read(ref _lastReconnectTicks);
            var previousReconnectTime = new DateTimeOffset(previousTicks, TimeSpan.Zero);
            TimeSpan elapsedSinceLastReconnect = utcNow - previousReconnectTime;

            // If multiple threads call ForceReconnect at the same time, we only want to honor one of them.
            if (elapsedSinceLastReconnect < ReconnectMinInterval)
            {
                return;
            }

            try
            {
                _reconnectSemaphore.Wait(RestartConnectionTimeout);
            }
            catch
            {
                // If we fail to enter the semaphore, then it is possible that another thread has already done so.
                // ForceReconnect() can be retried while connectivity problems persist.
                return;
            }

            try
            {
                utcNow = DateTimeOffset.UtcNow;
                elapsedSinceLastReconnect = utcNow - previousReconnectTime;

                if (_firstErrorTime == DateTimeOffset.MinValue)
                {
                    // We haven't seen an error since last reconnect, so set initial values.
                    _firstErrorTime = utcNow;
                    _previousErrorTime = utcNow;
                    return;
                }

                if (elapsedSinceLastReconnect < ReconnectMinInterval)
                {
                    return; // Some other thread made it through the check and the lock, so nothing to do.
                }

                TimeSpan elapsedSinceFirstError = utcNow - _firstErrorTime;
                TimeSpan elapsedSinceMostRecentError = utcNow - _previousErrorTime;

                bool shouldReconnect =
                    elapsedSinceFirstError >= ReconnectErrorThreshold // Make sure we gave the multiplexer enough time to reconnect on its own if it could.
                    && elapsedSinceMostRecentError <= ReconnectErrorThreshold; // Make sure we aren't working on stale data (e.g. if there was a gap in errors, don't reconnect yet).

                // Update the previousErrorTime timestamp to be now (e.g. this reconnect request).
                _previousErrorTime = utcNow;

                if (!shouldReconnect)
                {
                    return;
                }

                _firstErrorTime = DateTimeOffset.MinValue;
                _previousErrorTime = DateTimeOffset.MinValue;

                ConnectionMultiplexer oldConnection = connection;
                CloseConnection(oldConnection);
                connection = null;
                connection = CreateConnection();
                Interlocked.Exchange(ref _lastReconnectTicks, utcNow.UtcTicks);
            }
            finally
            {
                _reconnectSemaphore.Release();
            }
        }

        // This method may return null if it fails to acquire the semaphore in time.
        // Use the return value to update the "connection" field
        private static ConnectionMultiplexer CreateConnection()
        {
            if (connection != null)
            {
                // If we already have a good connection, let's re-use it
                return connection;
            }

            try
            {
                _initSemaphore.Wait(RestartConnectionTimeout);
            }
            catch
            {
                // We failed to enter the semaphore in the given amount of time. Connection will either be null, or have a value that was created by another thread.
                return connection;
            }

            // We entered the semaphore successfully.
            try
            {
                if (connection != null)
                {
                    // Another thread must have finished creating a new connection while we were waiting to enter the semaphore. Let's use it
                    return connection;
                }

                // Otherwise, we really need to create a new connection.
                return ConnectionMultiplexer.Connect(_connectionString);
            }
            finally
            {
                _initSemaphore.Release();
            }
        }

        private static void CloseConnection(ConnectionMultiplexer oldConnection)
        {
            if (oldConnection == null)
            {
                return;
            }
            try
            {
                oldConnection.Close();
            }
            catch (Exception)
            {
                // Ignore any errors from the oldConnection
            }
        }
    }
}