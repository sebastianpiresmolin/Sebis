using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Concurrent;

TcpListener server = new TcpListener(IPAddress.Any, 6379);

server.Start();
Console.WriteLine("Server started. Waiting for clients to connect...");

var store = new ConcurrentDictionary<string, string>(); // In-memory key-value store
var expirationTimes = new ConcurrentDictionary<string, long>(); // Key expiration times

_ = Task.Run(async () =>
{
    while (true)
    {
        foreach (var key in expirationTimes.Keys)
        {
            // Check if key expired and remove it
            if (expirationTimes.TryGetValue(key, out long expirationTime)
            && DateTime.UtcNow > DateTimeOffset.FromUnixTimeSeconds(expirationTime).UtcDateTime)
            {
                store.TryRemove(key, out _);
                expirationTimes.TryRemove(key, out _);
                Console.WriteLine($"Key {key} expired and removed.");
            }
        }
        await Task.Delay(100); // Check every 100ms
    }
});

while (true) // Accept multiple clients
{
    var clientSocket = await server.AcceptSocketAsync();
    Console.WriteLine("Client connected.");

    _ = Task.Run(async () => // Handle client in a separate task
    {
        byte[] buffer = new byte[1024];
        while (clientSocket.Connected)
        {
            int bytesRead = await clientSocket.ReceiveAsync(buffer, SocketFlags.None);
            if (bytesRead == 0)
            {
                Console.WriteLine("Client disconnected.");
                clientSocket.Close();
                break;
            }

            string request = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
            Console.WriteLine($"Received: {request}");

            var commandElements = ParseRESPArray(request);
            if (commandElements != null && commandElements.Length > 0)
            {
                string command = commandElements[0].ToUpper();
                if (command == "PING")
                {
                    byte[] response = Encoding.UTF8.GetBytes("+PONG\r\n");
                    await clientSocket.SendAsync(response, SocketFlags.None);
                    Console.WriteLine("Response sent: +PONG");
                }
                else if (command == "ECHO" && commandElements.Length == 2)
                {
                    string message = commandElements[1];
                    byte[] response = Encoding.UTF8.GetBytes($"${message.Length}\r\n{message}\r\n");
                    await clientSocket.SendAsync(response, SocketFlags.None);
                    Console.WriteLine($"Response sent: {message}");
                }
                else if (command == "SET" && (commandElements.Length == 3 || commandElements.Length == 5))
                {
                    string key = commandElements[1];
                    string value = commandElements[2];
                    store[key] = value;

                    if (commandElements.Length == 5 && commandElements[3].ToUpper() ==
                    "PX" && int.TryParse(commandElements[4], out int expirationTimeMs))
                    {
                        expirationTimes[key] = new DateTimeOffset(DateTime.UtcNow.AddMilliseconds(expirationTimeMs)).ToUnixTimeSeconds();
                        Console.WriteLine($"SET {key} {value} with expiration {expirationTimeMs}ms");
                    }
                    else
                    {
                        expirationTimes.TryRemove(key, out _);
                        Console.WriteLine($"SET {key} {value} without expiration");
                    }

                    byte[] response = Encoding.UTF8.GetBytes("+OK\r\n");
                    await clientSocket.SendAsync(response, SocketFlags.None);
                }
                else if (command == "GET" && commandElements.Length == 2)
                {
                    string key = commandElements[1];
                    if (store.TryGetValue(key, out string value))
                    {
                        byte[] response = Encoding.UTF8.GetBytes($"${value.Length}\r\n{value}\r\n");
                        await clientSocket.SendAsync(response, SocketFlags.None);
                        Console.WriteLine($"GET {key} -> {value}");
                    }
                    else
                    {
                        byte[] response = Encoding.UTF8.GetBytes("$-1\r\n");
                        await clientSocket.SendAsync(response, SocketFlags.None);
                        Console.WriteLine($"GET {key} -> (nil)");
                    }
                }
                else
                {
                    Console.WriteLine($"Unknown command: {command}");
                }
            }
        }
    });
}

string[] ParseRESPArray(string request)
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