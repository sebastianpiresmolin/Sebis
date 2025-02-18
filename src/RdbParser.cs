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
                int currentPosition = (int)reader.BaseStream.Position;
                byte opcode = reader.ReadByte();
                Console.WriteLine($"Processing opcode {opcode:X2} at position {currentPosition}");

                switch (opcode)
                {
                    case 0xFF: return; // EOF
                    case 0xFA:
                        SkipAuxiliaryField(reader);
                        break;
                    case 0xFE:
                        ProcessDatabaseSection(reader);
                        break;
                    default:
                        throw new InvalidDataException($"Unexpected opcode: {opcode:X2}");
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
        if (!Encoding.ASCII.GetString(header).StartsWith("REDIS"))
            throw new InvalidDataException("Invalid RDB header");

        Console.WriteLine($"RDB version: {Encoding.ASCII.GetString(reader.ReadBytes(4))}");
    }

    private void ProcessDatabaseSection(BinaryReader reader)
    {
        // Skip database number
        _ = ReadLengthEncoded(reader);

        // Handle resizedb information
        if (reader.PeekChar() == 0xFB)
        {
            reader.ReadByte(); // Consume FB
            _ = ReadLengthEncoded(reader); // hash_size
            _ = ReadLengthEncoded(reader); // expire_size
            Console.WriteLine("Processed resize DB information");
        }

        while (true)
        {
            int positionBeforeMarker = (int)reader.BaseStream.Position;
            byte marker = reader.ReadByte();

            if (marker == 0xFF)
            {
                Console.WriteLine("End of DB section");
                break;
            }

            long expiryMs = -1;

            // Handle expiration timestamps
            switch (marker)
            {
                case 0xFD:
                    expiryMs = reader.ReadUInt32() * 1000L;
                    marker = reader.ReadByte();
                    Console.WriteLine($"Found expiration timestamp ({expiryMs}ms)");
                    break;
                case 0xFC:
                    expiryMs = (long)reader.ReadUInt64();
                    marker = reader.ReadByte();
                    Console.WriteLine($"Found expiration timestamp ({expiryMs}ms)");
                    break;
            }

            // Only handle STRING types for CodeCrafters requirements
            if (marker != 0x00)
            {
                Console.WriteLine($"Skipping non-string type {marker:X2} at position {positionBeforeMarker}");
                continue;
            }

            string key = ReadRedisString(reader);
            string value = ReadRedisString(reader);

            _store[key] = value;
            Console.WriteLine($"Loaded key '{key}' with{(expiryMs > -1 ? "" : "out")} expiration");

            if (expiryMs > -1)
                _expirations[key] = expiryMs;
        }
    }

    private void SkipAuxiliaryField(BinaryReader reader)
    {
        Console.WriteLine("Skipping auxiliary field:");
        Console.WriteLine("- Key: " + ReadRedisString(reader));
        Console.WriteLine("- Value: " + ReadRedisString(reader));
    }

    private long ReadLengthEncoded(BinaryReader reader)
    {
        byte firstByte = reader.ReadByte();
        int prefix = firstByte >> 6;

        switch (prefix)
        {
            case < 3:
                return prefix switch
                {
                    0 => firstByte & 63,
                    1 => ((firstByte & 63) << 8) + reader.ReadByte(),
                    2 => BitConverter.ToUInt32(new byte[] // Changed to byte[]
                    {
                    (byte)(firstByte & 63), // Explicit cast
                    (byte)reader.ReadByte(),
                    (byte)reader.ReadByte(),
                    (byte)reader.ReadByte()
                    }, 0), // Start index 0 for big-endian conversion
                    _ => throw new InvalidDataException()
                };
            default:
                int encodingType = firstByte & 63;
                return encodingType switch
                {
                    0 => -1,
                    1 => -2,
                    2 => -3,
                    3 => throw new NotSupportedException("LZF compression"),
                    _ => throw new NotSupportedException($"Unknown encoding {encodingType}")
                };
        }
    }

    private string ReadRedisString(BinaryReader reader)
    {
        long length = ReadLengthEncoded(reader);

        return length switch
        {
            > 0 => Encoding.UTF8.GetString(reader.ReadBytes((int)length)),
            -1 => reader.ReadByte().ToString(),      // Handle  8-bit ints like redis-bits value "64"
            -2 => BitConverter.ToUInt16(reader.ReadBytes(2).Reverse().ToArray(), 0).ToString(),
            -3 => BitConverter.ToUInt32(reader.ReadBytes(4).Reverse().ToArray(), 0).ToString(),
            _ => throw new InvalidDataException($"Invalid length: {length}")
        };
    }
}