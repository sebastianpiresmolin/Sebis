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

        private Dictionary<string, string> data = new Dictionary<string, string>(); // Maybe need to use MemoryCache

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

            Task.Delay(expiry).ContinueWith(t => { RemoveFromData(key); });
        }

        public bool TryGetFromDataByKey(string key, out string value)
        {
            if (data.ContainsKey(key))
            {
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
            if (!data.ContainsKey(key))
            {
                return;
            }
            else
            {
                data.Remove(key);
            }
        }

        public void ClearAllData()
        {
            data.Clear();
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