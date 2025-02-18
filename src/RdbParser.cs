using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public class RdbParser
{
    private readonly ConcurrentDictionary<string, string> _store;
    private readonly ConcurrentDictionary<string, long> _expirations;

    public RdbParser(ConcurrentDictionary<string, string> store,
                    ConcurrentDictionary<string, long> expirations)
    {
        _store = store;
        _expirations = expirations;
    }

    public void Parse(string filePath)
    {
        byte[] data = File.ReadAllBytes(filePath);
        string hexString = BitConverter.ToString(data).Replace("-", "");

        if (!hexString.StartsWith("524544495330303131")) // REDIS0011 header
            throw new InvalidDataException("Invalid RDB header");

        int fbIndex = hexString.IndexOf("FB");
        if (fbIndex == -1) return;

        // Get number of key-value pairs
        int count = Convert.ToInt32(hexString.Substring(fbIndex + 4, 2), 16);

        string dbSection = hexString[(fbIndex + 6)..];
        for (int i = 0; i < count; i++)
        {
            if (dbSection.StartsWith("00")) // String type
            {
                dbSection = dbSection[2..];
                ReadKeyValue(ref dbSection);
            }
            else if (dbSection.StartsWith("FC")) // Expiry milliseconds
            {
                dbSection = dbSection[2..];
                long expiry = long.Parse(dbSection[..16], NumberStyles.HexNumber);
                dbSection = dbSection[16..];
                ReadKeyValue(ref dbSection, expiry);
            }
            else if (dbSection.StartsWith("FD")) // Expiry seconds
            {
                dbSection = dbSection[2..];
                long expiry = long.Parse(dbSection[..8], NumberStyles.HexNumber) * 1000;
                dbSection = dbSection[8..];
                ReadKeyValue(ref dbSection, expiry);
            }
        }
    }

    private void ReadKeyValue(ref string hexData, long expiryMs = -1)
    {
        int keyLen = Convert.ToInt32(hexData[..2], 16);
        hexData = hexData[2..];

        string key = HexToString(hexData[..(keyLen * 2)]);
        hexData = hexData[(keyLen * 2)..];

        int valLen = Convert.ToInt32(hexData[..2], 16);
        hexData = hexData[2..];

        string value = HexToString(hexData[..(valLen * 2)]);
        hexData = hexData[(valLen * 2)..];

        _store[key] = value;
        if (expiryMs > -1) _expirations[key] = expiryMs;
    }

    private static string HexToString(string hex)
    {
        byte[] bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
            bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);

        return Encoding.UTF8.GetString(bytes);
    }
}