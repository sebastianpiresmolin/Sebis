using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;

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
        using var stream = File.OpenRead(filePath);
        using var reader = new BinaryReader(stream);

        ValidateHeader(reader);
        Console.WriteLine("RDB header validated");

        try
        {
            while (true)
            {
                byte opcode = reader.ReadByte();

                switch (opcode)
                {
                    case 0xFF: return; // EOF
                    case 0xFA: SkipAuxiliaryField(reader); break;
                    case 0xFE: ProcessDatabaseSection(reader); break;
                    default: throw new InvalidDataException($"Unexpected opcode: {opcode:X2}");
                }
            }
        }
        catch (EndOfStreamException)
        {
            Console.WriteLine("End of RDB file reached");
        }
    }

    private void ValidateHeader(BinaryReader reader)
    {
        byte[] header = reader.ReadBytes(5);
        if (Encoding.ASCII.GetString(header) != "REDIS")
            throw new InvalidDataException("Invalid RDB header");

        byte[] version = reader.ReadBytes(4);
        Console.WriteLine($"RDB version: {Encoding.ASCII.GetString(version)}");
    }

    private void ProcessDatabaseSection(BinaryReader reader)
    {
        ReadLengthEncoded(reader); // Skip database number

        // Handle resizedb info
        if (reader.PeekChar() == 0xFB)
        {
            reader.ReadByte(); // Consume FB
            ReadLengthEncoded(reader); // hashSize
            ReadLengthEncoded(reader); // expireSize
        }

        while (true)
        {
            byte marker = reader.ReadByte();
            if (marker == 0xFF) break;

            long expiryMs = -1;

            // Handle expiration markers
            if (marker == 0xFD) // Seconds precision
            {
                expiryMs = reader.ReadUInt32() * 1000L;
                marker = reader.ReadByte();
            }
            else if (marker == 0xFC) // Milliseconds precision
            {
                expiryMs = (long)reader.ReadUInt64();
                marker = reader.ReadByte();
            }

            if (marker != 0x00) // Only handle string values
                continue;

            string key = ReadRedisString(reader);
            string value = ReadRedisString(reader);

            _store[key] = value;
            Console.WriteLine($"Loaded key: {key}");

            if (expiryMs != -1)
                _expirations[key] = expiryMs;
        }
    }

    private void SkipAuxiliaryField(BinaryReader reader)
    {
        ReadRedisString(reader); // Key
        ReadRedisString(reader); // Value
        Console.WriteLine("Skipped auxiliary field");
    }

    private long ReadLengthEncoded(BinaryReader reader)
    {
        byte firstByte = reader.ReadByte();
        int prefix = firstByte >> 6;

        switch (prefix)
        {
            case 0: return firstByte & 0x3F;
            case 1: return ((firstByte & 0x3F) << 8) | reader.ReadByte();
            case 2:
                byte[] bytes = reader.ReadBytes(4);
                if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
                return BitConverter.ToUInt32(bytes);
            case 3:
                int encodingType = firstByte & 0x3F;
                return encodingType switch
                {
                    0 => reader.ReadByte(),
                    1 => reader.ReadUInt16(),
                    2 => reader.ReadUInt32(),
                    _ => throw new NotSupportedException($"Special encoding {encodingType}")
                };
            default:
                throw new InvalidDataException("Invalid length encoding");
        }
    }

    private string ReadRedisString(BinaryReader reader)
    {
        long length = ReadLengthEncoded(reader);

        return length switch
        {
            -1 => throw new NotSupportedException("Compressed strings"),
            0 => string.Empty,
            _ => Encoding.UTF8.GetString(reader.ReadBytes((int)length))
        };
    }
}
