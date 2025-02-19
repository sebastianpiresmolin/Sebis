using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    class Storage
    {
        public static readonly Storage Instance = new Storage();

        private Dictionary<string, string> data = new Dictionary<string, string>();
        private Dictionary<string, DateTime> expiryTimes = new Dictionary<string, DateTime>();

        public void AddToData(string key, string value)
        {
            if (data.ContainsKey(key))
            {
                data[key] = value;
            }
            else
            {
                data.Add(key, value);
            }
        }

        public void AddToStorageWithExpiry(string key, string value, int expiry)
        {
            AddToData(key, value);
            DateTime expiryTime = DateTime.UtcNow.AddMilliseconds(expiry);
            expiryTimes[key] = expiryTime;

            Console.WriteLine($"Added key - {key} with expiry time - {expiryTime}");

            Task.Run(() => ExpiryTimer(expiry, key));
        }

        public bool TryGetFromDataByKey(string key, out string value)
        {
            if (data.ContainsKey(key))
            {
                if (expiryTimes.ContainsKey(key) && DateTime.UtcNow > expiryTimes[key])
                {
                    Console.WriteLine($"Key - {key} has expired at {expiryTimes[key]}");
                    RemoveFromData(key);
                    value = "";
                    return false;
                }

                value = data[key];
                return true;
            }
            else
            {
                value = "";
                return false;
            }
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

        private void ExpiryTimer(int expiry, string key)
        {
            Thread.Sleep(expiry);
            RemoveFromData(key);
        }

        public string[] GetAllKeys()
        {
            return data.Keys.ToArray();
        }
    }
}