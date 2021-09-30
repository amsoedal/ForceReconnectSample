// --------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// --------------------------------------------------------------------------------------------

using System;
using System.Threading;
using StackExchange.Redis;

namespace ForceReconnect
{
    internal static class Redis
    {
        private static long lastReconnectTicks = DateTimeOffset.MinValue.UtcTicks;
        private static DateTimeOffset firstErrorTime = DateTimeOffset.MinValue;
        private static DateTimeOffset previousErrorTime = DateTimeOffset.MinValue;

        private static SemaphoreSlim reconnectSemaphore = new SemaphoreSlim(initialCount: 1, maxCount: 1);
        private static SemaphoreSlim initSemaphore = new SemaphoreSlim(initialCount: 1, maxCount: 1);

        // In general, let StackExchange.Redis handle most reconnects,
        // so limit the frequency of how often this will actually reconnect.
        public static TimeSpan ReconnectMinFrequency = TimeSpan.FromSeconds(60);

        // if errors continue for longer than the below threshold, then the
        // multiplexer seems to not be reconnecting, so re-create the multiplexer
        public static TimeSpan ReconnectErrorThreshold = TimeSpan.FromSeconds(30);

        public static TimeSpan RestartConnectionTimeout = TimeSpan.FromSeconds(15);

        private static string connectionString = "TODO: CALL InitializeConnectionString() method with connection string";
        private static ConnectionMultiplexer connection = CreateConnection();

        public static ConnectionMultiplexer Connection { get { return connection; } }

        public static void InitializeConnectionString(string cnxString)
        {
            if (string.IsNullOrWhiteSpace(cnxString))
                throw new ArgumentNullException(nameof(cnxString));

            connectionString = cnxString;
        }

        /// <summary>
        /// Force a new ConnectionMultiplexer to be created.
        /// NOTES:
        ///     1. Users of the ConnectionMultiplexer MUST handle ObjectDisposedExceptions, which can now happen as a result of calling ForceReconnect().
        ///     2. Don't call ForceReconnect for Timeouts, just for RedisConnectionExceptions or SocketExceptions.
        ///     3. Call this method every time you see a connection exception. The code will:
        ///         a. wait to reconnect for at least the "ReconnectErrorThreshold" time of repeated errors before actually reconnecting
        ///         b. not reconnect more frequently than configured in "ReconnectMinFrequency"
        /// </summary>
        public static void ForceReconnect()
        {
            var utcNow = DateTimeOffset.UtcNow;
            long previousTicks = Interlocked.Read(ref lastReconnectTicks);
            var previousReconnectTime = new DateTimeOffset(previousTicks, TimeSpan.Zero);
            TimeSpan elapsedSinceLastReconnect = utcNow - previousReconnectTime;

            // If multiple threads call ForceReconnect at the same time, we only want to honor one of them.
            if (elapsedSinceLastReconnect < ReconnectMinFrequency)
            {
                return;
            }

            try
            {
                reconnectSemaphore.Wait(RestartConnectionTimeout);
            }
            catch
            {
                return;
            }

            try
            {
                utcNow = DateTimeOffset.UtcNow;
                elapsedSinceLastReconnect = utcNow - previousReconnectTime;

                if (firstErrorTime == DateTimeOffset.MinValue)
                {
                    // We haven't seen an error since last reconnect, so set initial values.
                    firstErrorTime = utcNow;
                    previousErrorTime = utcNow;
                    return;
                }

                if (elapsedSinceLastReconnect < ReconnectMinFrequency)
                    return; // Some other thread made it through the check and the lock, so nothing to do.

                TimeSpan elapsedSinceFirstError = utcNow - firstErrorTime;
                TimeSpan elapsedSinceMostRecentError = utcNow - previousErrorTime;

                bool shouldReconnect =
                    elapsedSinceFirstError >= ReconnectErrorThreshold // Make sure we gave the multiplexer enough time to reconnect on its own if it could.
                    && elapsedSinceMostRecentError <= ReconnectErrorThreshold; // Make sure we aren't working on stale data (e.g. if there was a gap in errors, don't reconnect yet).

                // Update the previousErrorTime timestamp to be now (e.g. this reconnect request).
                previousErrorTime = utcNow;

                if (!shouldReconnect)
                {
                    return;
                }

                firstErrorTime = DateTimeOffset.MinValue;
                previousErrorTime = DateTimeOffset.MinValue;

                ConnectionMultiplexer oldConnection = connection;
                CloseConnection(oldConnection);
                connection = null;
                CreateConnection();
                Interlocked.Exchange(ref lastReconnectTicks, utcNow.UtcTicks);
            }
            finally
            {
                reconnectSemaphore.Release();
            }
        }

        private static ConnectionMultiplexer CreateConnection()
        {
            if (connection != null)
            {
                // If we already have a good connection, let's re-use it
                return connection;
            }

            try
            {
                initSemaphore.Wait(RestartConnectionTimeout);
            }
            catch
            {
                // We failed to enter the semaphore. Connection will either be null, or have a value that was created by another thread.
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
                connection = ConnectionMultiplexer.Connect(connectionString);
                return connection;
            }
            finally
            {
                initSemaphore.Release();
            }
        }

        private static void CloseConnection(ConnectionMultiplexer oldConnection)
        {
            if (oldConnection != null)
            {
                try
                {
                    oldConnection.Close();
                }
                catch (Exception)
                {
                    // Example error condition: if accessing oldConnection causes a connection attempt and that fails.
                }
            }
        }
    }
}