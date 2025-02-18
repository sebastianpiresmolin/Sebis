public static class RESPParser
{
    public static string[] ParseRESPArray(string request)
    {
        if (request.StartsWith("*"))
        {
            int lengthEndIndex = request.IndexOf("\r\n");
            if (lengthEndIndex > 1)
            {
                string lengthStr = request.Substring(1, lengthEndIndex - 1);
                if (int.TryParse(lengthStr, out int length))
                {
                    string[] elements = new string[length];
                    int currentIndex = lengthEndIndex + 2;
                    for (int i = 0; i < length; i++)
                    {
                        if (request[currentIndex] == '$')
                        {
                            int bulkLengthEndIndex = request.IndexOf("\r\n", currentIndex);
                            string bulkLengthStr = request.Substring(currentIndex + 1, bulkLengthEndIndex - currentIndex - 1);
                            if (int.TryParse(bulkLengthStr, out int bulkLength))
                            {
                                currentIndex = bulkLengthEndIndex + 2;
                                elements[i] = request.Substring(currentIndex, bulkLength);
                                currentIndex += bulkLength + 2;
                            }
                        }
                    }
                    return elements;
                }
            }
        }
        return null;
    }
}