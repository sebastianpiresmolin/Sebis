using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    internal class Rdb
    {
        public static readonly Rdb Instance = new();

        private readonly Dictionary<string, string> config = [];

        public string GetConfigValueByKey(string key)
        {
            return config[key];
        }

        public void SetConfig(string key, string value)
        {
            config[key] = value;
        }

        public void ReadDb()
        {
            if (!config.ContainsKey("dir") || !config.ContainsKey("dbfilename"))
                return;

            string path = $"{config["dir"]}/{config["dbfilename"]}";
            if (!File.Exists(path)) return;

            byte[] data = File.ReadAllBytes(path);
            string hexString = BitConverter.ToString(data).Replace("-", "");

            int fbIndex = hexString.IndexOf("FB");
            if (fbIndex == -1) return;

            int count = Convert.ToInt32(hexString.Substring(fbIndex + 4, 2), 16);
            string dbSection = hexString[(fbIndex + 6)..];

            for (int i = 0; i < count; i++)
            {
                if (dbSection.StartsWith("FC")) // Millisecond expiry
                {
                    dbSection = dbSection[2..];
                    long expiryMs = long.Parse(dbSection[..16], NumberStyles.HexNumber);
                    dbSection = dbSection[16..];
                    dbSection = ProcessKeyValue(dbSection, (int)expiryMs);
                }
                else if (dbSection.StartsWith("FD")) // Second expiry
                {
                    dbSection = dbSection[2..];
                    long expiryMs = long.Parse(dbSection[..8], NumberStyles.HexNumber) * 1000;
                    dbSection = dbSection[8..];
                    dbSection = ProcessKeyValue(dbSection, (int)expiryMs);
                }
                else
                {
                    dbSection = ProcessKeyValue(dbSection);
                }
            }
        }

        private string ProcessKeyValue(string hexData, long expiryMs = -1)
        {
            hexData = hexData[2..]; // Skip value type

            int keyLen = Convert.ToInt32(hexData[..2], 16);
            hexData = hexData[2..];

            string key = HexToString(hexData[..(keyLen * 2)]);
            hexData = hexData[(keyLen * 2)..];

            int valLen = Convert.ToInt32(hexData[..2], 16);
            hexData = hexData[2..];

            string value = HexToString(hexData[..(valLen * 2)]);
            hexData = hexData[(valLen * 2)..];

            if (expiryMs > 0)
            {
                Storage.Instance.AddToStorageWithExpiry(key, value, (int)expiryMs);
                Console.WriteLine($"Added {key} with {expiryMs}ms expiry");
            }
            else
            {
                Storage.Instance.AddToData(key, value);
            }

            return hexData;
        }

        private static string HexToString(string hex)
        {
            byte[] bytes = new byte[hex.Length / 2];
            for (int i = 0; i < bytes.Length; i++)
                bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);

            return Encoding.UTF8.GetString(bytes);
        }
    }
}