using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    class Storage
    {
        public static readonly Storage Instance = new();
        private readonly Dictionary<string, string> data = new();
        private readonly Dictionary<string, DateTime> expiryTimes = new();

        public void AddToData(string key, string value)
        {
            data[key] = value;
            expiryTimes.Remove(key);
        }

        public void AddToStorageWithExpiry(string key, string value, int expiryMs)
        {
            data[key] = value;
            var expiryTime = DateTime.UtcNow.AddMilliseconds(expiryMs);
            expiryTimes[key] = expiryTime;

            Task.Run(() => ExpiryTimer(expiryMs, key));
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

        public void RemoveFromData(string key)
        {
            if (data.ContainsKey(key))
            {
                data.Remove(key);
                expiryTimes.Remove(key);
            }
        }

        public void ClearAllData()
        {
            data.Clear();
            expiryTimes.Clear();
        }

        public Dictionary<string, string> GetAllData()
        {
            return data;
        }

        private void ExpiryTimer(int delayMs, string key)
        {
            Thread.Sleep(delayMs);
            RemoveFromData(key);
        }

        public string[] GetAllKeys()
        {
            return data.Keys.ToArray();
        }
    }
}