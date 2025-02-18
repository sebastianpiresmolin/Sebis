using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

public class CommandHandler
{
    private readonly ConcurrentDictionary<string, string> store;
    private readonly ConcurrentDictionary<string, long> expirationTimes;
    private readonly ConcurrentDictionary<string, string> config;

    public CommandHandler(ConcurrentDictionary<string, string> store, ConcurrentDictionary<string, long> expirationTimes, ConcurrentDictionary<string, string> config)
    {
        this.store = store;
        this.expirationTimes = expirationTimes;
        this.config = config;
    }

    public async Task HandleCommand(string[] commandElements, Socket clientSocket)
    {
        if (commandElements == null || commandElements.Length == 0)
        {
            Console.WriteLine("Invalid command.");
            return;
        }

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

            if (commandElements.Length == 5 && commandElements[3].ToUpper() == "PX" && int.TryParse(commandElements[4], out int expirationTimeMs))
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
        else if (command == "CONFIG" && commandElements.Length == 3 && commandElements[1].ToUpper() == "GET")
        {
            string configKey = commandElements[2];
            if (config.TryGetValue(configKey, out string configValue))
            {
                byte[] response = Encoding.UTF8.GetBytes($"*2\r\n${configKey.Length}\r\n{configKey}\r\n${configValue.Length}\r\n{configValue}\r\n");
                await clientSocket.SendAsync(response, SocketFlags.None);
                Console.WriteLine($"CONFIG GET {configKey} -> {configValue}");
            }
            else
            {
                byte[] response = Encoding.UTF8.GetBytes("$-1\r\n");
                await clientSocket.SendAsync(response, SocketFlags.None);
                Console.WriteLine($"CONFIG GET {configKey} -> (nil)");
            }
        }
        else
        {
            Console.WriteLine($"Unknown command: {command}");
        }
    }
}