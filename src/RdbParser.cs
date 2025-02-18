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

        while (true)
        {
            byte opcode = reader.ReadByte();
            if (opcode == 0xFF) // EOF
                break;

            if (opcode == 0xFE) // Database selector
            {
                ProcessDatabaseSection(reader, store, expirationTimes);
            }
            else if (opcode == 0xFA) // Auxiliary metadata
            {
                SkipAuxiliaryFields(reader);
            }
        }
    }

    private void ValidateHeader(BinaryReader reader)
    {
        string magic = Encoding.ASCII.GetString(reader.ReadBytes(5));
        if (magic != "REDIS")
            throw new InvalidDataException("Invalid RDB file format");

        string version = Encoding.ASCII.GetString(reader.ReadBytes(4));
    }

    private void ProcessDatabaseSection(BinaryReader reader, ConcurrentDictionary<string, string> store, ConcurrentDictionary<string, long> expirationTimes)
    {
        var dbIndex = ReadLengthEncoded(reader);

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

        while (true)
        {
            byte b = reader.ReadByte();
            if (b == 0xFF || b == 0xFE)
                break; // End of database or EOF

            long expiryMs = -1;
            if (b == 0xFD)
            {
                uint expirySeconds = reader.ReadUInt32();
                expiryMs = expirySeconds * 1000;
                b = reader.ReadByte();
            }
            else if (b == 0xFC)
            {
                expiryMs = (long)reader.ReadUInt64();
                b = reader.ReadByte();
            }

            string key = ReadRedisString(reader);
            string value = ReadRedisString(reader);

            store[key] = value;
            if (expiryMs != -1)
            {
                expirationTimes[key] = expiryMs;
            }
        }
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
            default:
                throw new NotSupportedException("Special encodings not supported");
        }
    }

    private string ReadRedisString(BinaryReader reader)
    {
        long length = ReadLengthEncoded(reader);
        byte[] bytes = reader.ReadBytes((int)length);
        return Encoding.UTF8.GetString(bytes);
    }
}

