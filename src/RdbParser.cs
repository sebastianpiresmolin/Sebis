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
        Console.WriteLine($"RDB file header validated");

        try
        {
            while (true)
            {
                byte opcode = reader.ReadByte();

                switch (opcode)
                {
                    case 0xFF: // EOF
                        return;

                    case 0xFA: // Auxiliary field
                        SkipAuxiliaryField(reader);
                        break;

                    case 0xFE: // Database selector
                        ProcessDatabaseSection(reader);
                        break;

                    default:
                        throw new InvalidDataException($"Unexpected opcode: 0x{opcode:X2}");
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
        byte[] header = reader.ReadBytes(5);
        if (Encoding.ASCII.GetString(header) != "REDIS")
            throw new InvalidDataException("Invalid RDB file format");

        byte[] versionBytes = reader.ReadBytes(4);
        Console.WriteLine($"RDB version: {Encoding.ASCII.GetString(versionBytes)}");
    }

    private void ProcessDatabaseSection(BinaryReader reader)
    {
        ReadLengthEncoded(reader); // Skip database number

        // Handle resizedb info
        if (reader.PeekChar() == 0xFB)
        {
            reader.ReadByte(); // Consume 0xFB
            ReadLengthEncoded(reader); // hashSize
            ReadLengthEncoded(reader); // expireSize
        }

        while (true)
        {
            byte valueType = reader.ReadByte();
            if (valueType == 0xFF) break;

            long expiryMs = -1;

            // Handle expiration timestamps
            if (valueType == 0xFD) // Seconds precision
            {
                expiryMs = reader.ReadUInt32() * 1000L;
                valueType = reader.ReadByte();
            }
            else if (valueType == 0xFC) // Milliseconds precision
            {
                expiryMs = (long)reader.ReadUInt64();
                valueType = reader.ReadByte();
            }

            if (valueType != 0x00) // Only handle strings for CodeCrafters
                continue;

            string key = ReadRedisString(reader);
            string value = ReadRedisString(reader);

            _store[key] = value;
            Console.WriteLine($"Loaded key: {key}");

            if (expiryMs != -1)
            {
                _expirations[key] = expiryMs;
                Console.WriteLine($"Set expiry for {key}: {expiryMs}ms");
            }
        }
    }

    private void SkipAuxiliaryField(BinaryReader reader)
    {
        ReadRedisString(reader); // Skip key
        ReadRedisString(reader); // Skip value
        Console.WriteLine("Skipped auxiliary field");
    }

    private long ReadLengthEncoded(BinaryReader reader)
    {
        byte firstByte = reader.ReadByte();
        int prefix = firstByte >> 6;

        return prefix switch
        {
            0 => firstByte & 0x3F,
            1 => ((firstByte & 0x3F) << 8) | reader.ReadByte(),
            2 => ReadFourByteLength(reader),
            3 => HandleSpecialEncoding(firstByte, reader),
            _ => throw new InvalidDataException("Invalid length encoding")
        };
    }

    private long ReadFourByteLength(BinaryReader reader)
    {
        byte[] bytes = reader.ReadBytes(4);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);

        return BitConverter.ToUInt32(bytes);
    }

    private long HandleSpecialEncoding(byte firstByte, BinaryReader reader)
    {
        int encodingType = firstByte & 0x3F;

        return encodingType switch
        {
            0 => reader.ReadByte(),       // 8-bit integer
            1 => reader.ReadUInt16(),     // 16-bit integer
            2 => reader.ReadUInt32(),     // 32-bit integer
            3 => throw new NotSupportedException("LZF compression not supported"),
            _ => throw new NotSupportedException($"Unknown encoding: {encodingType}")
        };
    }

    private string ReadRedisString(BinaryReader reader)
    {
        long length = ReadLengthEncoded(reader);

        // Handle integer encodings
        if (length < 0)
            return (-length).ToString();

        byte[] buffer = new byte[length];
        reader.Read(buffer);

        return Encoding.UTF8.GetString(buffer);
    }
}
