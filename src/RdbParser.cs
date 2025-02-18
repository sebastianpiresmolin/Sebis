using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;

public class RdbParser
{
    public Dictionary<string, string> Parse(string filePath)
    {
        var keyValuePairs = new Dictionary<string, string>();
        byte[] data = File.ReadAllBytes(filePath);
        Console.WriteLine($"File read successfully. Data (hex): {BitConverter.ToString(data)}");

        int index = 0;
        try
        {
            // Skip the REDIS header (9 bytes)
            index += 9;

            while (index < data.Length)
            {
                if (data[index] == 0xFA) // Auxiliary field
                {
                    index = SkipAuxiliaryField(data, index);
                }
                else if (data[index] == 0xFE) // Database selector
                {
                    index = ParseDatabaseSection(data, index, keyValuePairs);
                }
                else if (data[index] == 0xFF) // EOF
                {
                    break;
                }
                else
                {
                    index++; // Skip unknown or unhandled sections
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parsing RDB data: {ex.Message}");
            throw;
        }

        return keyValuePairs;
    }

    private int SkipAuxiliaryField(byte[] data, int startIndex)
    {
        int index = startIndex + 1;
        // Skip key
        index = SkipString(data, index);
        // Skip value
        index = SkipString(data, index);
        return index;
    }

    private int ParseDatabaseSection(byte[] data, int startIndex, Dictionary<string, string> keyValuePairs)
    {
        int index = startIndex + 1;

        // Skip database number
        index = SkipLengthEncoded(data, index);

        if (data[index] == 0xFB) // Resizedb
        {
            index++;
            index = SkipLengthEncoded(data, index); // Skip hash size
            index = SkipLengthEncoded(data, index); // Skip expire size
        }

        while (index < data.Length && data[index] != 0xFF)
        {
            if (data[index] == 0xFD || data[index] == 0xFC) // Expiry
            {
                index += (data[index] == 0xFD) ? 5 : 9; // Skip expiry info
            }

            if (data[index] != 0x00) // Only handle string values for now
            {
                throw new NotSupportedException("Non-string types are not supported yet.");
            }
            index++;

            string key = ReadString(data, ref index);
            string value = ReadString(data, ref index);

            keyValuePairs[key] = value;
            Console.WriteLine($"Loaded key-value pair: {key} => {value}");
        }

        return index;
    }

    private int SkipLengthEncoded(byte[] data, int index)
    {
        byte firstByte = data[index];
        int prefix = firstByte >> 6;

        switch (prefix)
        {
            case 0: return index + 1;
            case 1: return index + 2;
            case 2: return index + 5;
            default: throw new NotSupportedException("Special length encodings are not supported.");
        }
    }

    private int SkipString(byte[] data, int index)
    {
        int length = ReadLength(data, ref index);
        return index + length;
    }

    private string ReadString(byte[] data, ref int index)
    {
        int length = ReadLength(data, ref index);
        string result = Encoding.UTF8.GetString(data, index, length);
        index += length;
        return result;
    }

    private int ReadLength(byte[] data, ref int index)
    {
        byte firstByte = data[index++];
        int prefix = firstByte >> 6;

        switch (prefix)
        {
            case 0: return firstByte & 0x3F;
            case 1: return ((firstByte & 0x3F) << 8) | data[index++];
            case 2: return BitConverter.ToInt32(data, index);
            default: throw new NotSupportedException("Special length encodings are not supported.");
        }
    }
}
