using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    class Storage
    {
        public static readonly Storage Instance = new();
        private readonly ConcurrentDictionary<string, string> data = new();
        private readonly ConcurrentDictionary<string, DateTime> expiryTimes = new();
        private readonly Timer cleanupTimer;

        private Storage()
        {
            // Cleanup expired items every 30 seconds
            cleanupTimer = new Timer(_ => CleanupExpiredItems(), null,
                TimeSpan.Zero, TimeSpan.FromSeconds(0.1));
        }

        public void AddToData(string key, string value)
        {
            data[key] = value;
            expiryTimes.TryRemove(key, out _);
        }

        public void AddToStorageWithExpiry(string key, string value, int expiryMs)
        {
            var expiryTime = DateTime.UtcNow.AddMilliseconds(expiryMs);

            data.AddOrUpdate(key, value, (_, _) => value);
            expiryTimes.AddOrUpdate(key, expiryTime, (_, _) => expiryTime);

            ScheduleExpiryCheck(key, expiryMs);
        }

        public bool TryGetFromDataByKey(string key, out string? value)
        {
            if (expiryTimes.TryGetValue(key, out DateTime expiry) &&
                DateTime.UtcNow > expiry)
            {
                RemoveFromData(key);
                value = null;
                return false;
            }
            return data.TryGetValue(key, out value);
        }

        private void ScheduleExpiryCheck(string key, int delayMs)
        {
            Task.Delay(delayMs).ContinueWith(_ =>
            {
                if (expiryTimes.TryGetValue(key, out var expiry) &&
                    DateTime.UtcNow >= expiry)
                {
                    RemoveFromData(key);
                }
            });
        }

        private void CleanupExpiredItems()
        {
            var now = DateTime.UtcNow;
            foreach (var kvp in expiryTimes)
            {
                if (now > kvp.Value)
                {
                    RemoveFromData(kvp.Key);
                }
            }
        }

        private void RemoveFromData(string key)
        {
            data.TryRemove(key, out _);
            expiryTimes.TryRemove(key, out _);
        }

        public string[] GetAllKeys() => data.Keys.ToArray();
    }
}
