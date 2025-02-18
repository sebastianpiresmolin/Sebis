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
        var data = File.ReadAllBytes(filePath);
        int index = 9; // Skip REDIS0011 header

        while (index < data.Length)
        {
            switch (data[index])
            {
                case 0xFA: // Auxiliary field
                    index = SkipAuxiliaryField(data, index + 1);
                    break;

                case 0xFE: // Database selector
                    index = ProcessDatabaseSection(data, index + 1);
                    break;

                case 0xFF: // EOF
                    return;

                default:
                    index++;
                    break;
            }
        }
    }

    private int ProcessDatabaseSection(byte[] data, int index)
    {
        // Skip database number
        index = SkipLengthEncoded(data, index);

        if (data[index] == 0xFB) // Resizedb info
        {
            index++;
            index = SkipLengthEncoded(data, index); // hashSize
            index = SkipLengthEncoded(data, index); // expireSize
        }

        while (index < data.Length && data[index] != 0xFF)
        {
            long expiryMs = -1;

            if (data[index] == 0xFD) // Expire seconds
            {
                expiryMs = BitConverter.ToUInt32(data, index + 1) * 1000L;
                index += 5;
            }
            else if (data[index] == 0xFC) // Expire milliseconds
            {
                expiryMs = BitConverter.ToInt64(data, index + 1);
                index += 9;
            }

            if (data[index] == 0x00) // String type
            {
                index++;
                var key = ReadString(data, ref index);
                var value = ReadString(data, ref index);

                _store[key] = value;
                if (expiryMs != -1)
                {
                    _expirations[key] = expiryMs;
                }
            }
            else
            {
                index++;
            }
        }

        return index + 1; // Skip EOF byte
    }

    private string ReadString(byte[] data, ref int index)
    {
        int length = ReadLength(data[index..], out int bytesConsumed);
        index += bytesConsumed;

        var value = Encoding.UTF8.GetString(data, index, length);
        index += length;
        return value;
    }

    private int ReadLength(byte[] data, out int bytesConsumed)
    {
        byte firstByte = data[0];
        bytesConsumed = 1;

        switch (firstByte >> 6)
        {
            case 0:
                return firstByte & 0x3F;

            case 1:
                bytesConsumed++;
                return ((firstByte & 0x3F) << 8) | data[1];

            case 2:
                bytesConsumed += 4;
                return BitConverter.ToInt32(new byte[] {
                    data[4], data[3], data[2], data[1] }, 0); // Big-endian

            default:
                throw new NotSupportedException("Special encodings not supported");
        }
    }

    private int SkipAuxiliaryField(byte[] data, int index)
    {
        // Skip key
        int keyLen = ReadLength(data[index..], out int consumed);
        index += consumed + keyLen;

        // Skip value 
        int valLen = ReadLength(data[index..], out consumed);
        return index + consumed + valLen;
    }

    private int SkipLengthEncoded(byte[] data, int index)
    {
        switch (data[index] >> 6)
        {
            case 0: return index + 1;
            case 1: return index + 2;
            case 2: return index + 5;
            default: throw new NotSupportedException();
        }
    }
}
