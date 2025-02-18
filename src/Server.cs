using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("Logs from your program will appear here!");


TcpListener server = new TcpListener(IPAddress.Any, 6379);

server.Start();
Console.WriteLine("Server started. Waiting for clients to connect...");


while (true)
{
    var clientSocket = await server.AcceptSocketAsync();
    Console.WriteLine("Client connected.");

    _ = Task.Run(async () =>
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

            // Parse RESP formatted request
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