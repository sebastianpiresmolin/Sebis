using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Concurrent;

TcpListener server = new TcpListener(IPAddress.Any, 6379);

server.Start();
Console.WriteLine("Server started. Waiting for clients to connect...");

var store = new ConcurrentDictionary<string, string>(); // In-memory key-value store
var expirationTimes = new ConcurrentDictionary<string, long>(); // Key expiration times
var commandHandler = new CommandHandler(store, expirationTimes);

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

            var commandElements = RESPParser.ParseRESPArray(request);
            await commandHandler.HandleCommand(commandElements, clientSocket);
        }
    });
}