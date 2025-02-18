using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;

public class RdbParser
{
    public void Parse(string filePath, ConcurrentDictionary<string, string> store, ConcurrentDictionary<string, long> expirationTimes)
    {
        using var stream = File.OpenRead(filePath);
        using var reader = new BinaryReader(stream);

        ValidateHeader(reader);
        Console.WriteLine("RDB header validated successfully");

        try
        {
            while (true)
            {
                byte opcode = reader.ReadByte();
                if (opcode == 0xFF) // EOF
                {
                    Console.WriteLine("Reached EOF marker");
                    break;
                }

                if (opcode == 0xFE) // Database selector
                {
                    Console.WriteLine("Processing database section");
                    ProcessDatabaseSection(reader, store, expirationTimes);
                }
                else if (opcode == 0xFA) // Auxiliary metadata
                {
                    Console.WriteLine("Skipping auxiliary fields");
                    SkipAuxiliaryFields(reader);
                }
                else
                {
                    Console.WriteLine($"Unknown opcode: {opcode:X2}");
                }
            }
        }
        catch (EndOfStreamException)
        {
            Console.WriteLine("Reached end of RDB file");
        }
    }

    private void ValidateHeader(BinaryReader reader)
    {
        string magic = Encoding.ASCII.GetString(reader.ReadBytes(5));
        if (magic != "REDIS")
            throw new InvalidDataException("Invalid RDB file format");

        string version = Encoding.ASCII.GetString(reader.ReadBytes(4));
        Console.WriteLine($"RDB version: {version}");
    }

    private void ProcessDatabaseSection(BinaryReader reader, ConcurrentDictionary<string, string> store, ConcurrentDictionary<string, long> expirationTimes)
    {
        ReadLengthEncoded(reader); // Read and ignore database number

        // Skip resizedb info if present
        byte next = reader.ReadByte();
        if (next == 0xFB)
        {
            ReadLengthEncoded(reader); // hashSize
            ReadLengthEncoded(reader); // expireSize
        }
        else
        {
            reader.BaseStream.Position--;
        }

        // Read key-value pair
        byte valueType = reader.ReadByte();
        if (valueType != 0) // 0 is for string encoding
        {
            throw new NotSupportedException($"Unsupported value type: {valueType}");
        }

        string key = ReadRedisString(reader);
        string value = ReadRedisString(reader);

        store[key] = value;
        Console.WriteLine($"Loaded key: {key}, value: {value}");
    }

    private void SkipAuxiliaryFields(BinaryReader reader)
    {
        ReadRedisString(reader); // Skip key
        ReadRedisString(reader); // Skip value
    }

    private long ReadLengthEncoded(BinaryReader reader)
    {
        byte firstByte = reader.ReadByte();
        int prefix = firstByte >> 6;

        switch (prefix)
        {
            case 0:
                return firstByte & 0x3F;
            case 1:
                byte secondByte = reader.ReadByte();
                return ((firstByte & 0x3F) << 8) | secondByte;
            case 2:
                byte[] fourBytes = reader.ReadBytes(4);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(fourBytes);
                return BitConverter.ToUInt32(fourBytes, 0);
            case 3:
                int encoding = firstByte & 0x3F;
                switch (encoding)
                {
                    case 0: // 8 bit integer
                        return reader.ReadByte();
                    case 1: // 16 bit integer
                        return BitConverter.ToUInt16(reader.ReadBytes(2).Reverse().ToArray(), 0);
                    case 2: // 32 bit integer
                        return BitConverter.ToUInt32(reader.ReadBytes(4).Reverse().ToArray(), 0);
                    default:
                        throw new NotSupportedException($"Unknown string encoding: {encoding}");
                }
            default:
                throw new InvalidDataException("Invalid length encoding");
        }
    }

    private string ReadRedisString(BinaryReader reader)
    {
        long length = ReadLengthEncoded(reader);
        if (length >= 0 && length <= int.MaxValue)
        {
            byte[] bytes = reader.ReadBytes((int)length);
            return Encoding.UTF8.GetString(bytes);
        }
        else
        {
            throw new NotSupportedException($"Unsupported string length: {length}");
        }
    }
}
