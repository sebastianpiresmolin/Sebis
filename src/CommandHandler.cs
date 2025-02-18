using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    internal class CommandHandler
    {
        public static void HandleCommandArray(Socket socket, string[] commands)
        {
            int pointer = 0;
            while (pointer < commands.Length)
            {
                //Console.WriteLine(commands[pointer]);
                switch (commands[pointer])
                {
                    case "ECHO":
                        EchoCommand(socket, commands[++pointer]);
                        break;
                    case "PING":
                        PingCommand(socket);
                        break;
                    case "SET":
                        {
                            string key = commands[++pointer];
                            string value = commands[++pointer];

                            //check if SET getting PX argument
                            if (pointer < commands.Length - 1 && commands[pointer + 1] == "px")
                            {
                                pointer++;
                                int px = int.Parse(commands[++pointer]); // in milliseconds
                                SetCommand(socket, key, value, px);
                            }
                            else
                            {
                                SetCommand(socket, key, value);
                            }
                        }
                        break;
                    case "GET":
                        {
                            string key = commands[++pointer];
                            GetCommand(socket, key);
                        }
                        break;
                    case "CONFIG":
                        if (commands[pointer + 1] == "GET")
                        {
                            pointer++;
                            string key = commands[++pointer];
                            ConfigGetCommand(socket, key);
                        }
                        break;
                    case "KEYS":
                        if (commands[++pointer] == "*")
                        {
                            KeysCommand(socket);
                        }
                        break;
                }
                pointer++;
            }
        }

        private static void EchoCommand(Socket socket, string echoText)
        {
            string msg = Resp.MakeBulkString(echoText);
            Console.WriteLine($"Sending echo message - {msg}");
            socket.SendAsync(Encoding.UTF8.GetBytes(msg));
        }

        private static void PingCommand(Socket socket)
        {
            string msg = Resp.MakeSimpleString("PONG");
            Console.WriteLine($"Sending pong message - {msg}");
            socket.SendAsync(Encoding.UTF8.GetBytes(msg));
        }

        private static void SetCommand(Socket socket, string key, string value, int px = -1)
        {
            if (px > 0)
            {
                Storage.Instance.AddToStorageWithExpiry(key, value, px);
                Console.WriteLine($"Set key - {key} with value - {value}, px - {px}");
            }
            else
            {
                Storage.Instance.AddToData(key, value);
                Console.WriteLine($"Set key - {key} with value - {value}");
            }

            string msg = Resp.MakeSimpleString("OK");
            Console.WriteLine($"Sending OK message - {msg}");
            socket.SendAsync(Encoding.UTF8.GetBytes(msg));
        }

        private static void GetCommand(Socket socket, string key)
        {
            if (Storage.Instance.TryGetFromDataByKey(key, out string value))
            {
                string msg = Resp.MakeBulkString(value);
                Console.WriteLine($"Sending value message - {msg}");
                socket.SendAsync(Encoding.UTF8.GetBytes(msg));
            }
            else
            {
                string msg = Resp.MakeNullBulkString();
                Console.WriteLine($"Sending null value message - {msg}");
                socket.SendAsync(Encoding.UTF8.GetBytes(msg));
            }
        }

        private static void ConfigGetCommand(Socket socket, string key)
        {
            string value = Rdb.Instance.GetConfigValueByKey(key);
            string msg = Resp.MakeArray(new string[] { key, value });
            Console.WriteLine($"Sending config value message - {msg}");
            socket.SendAsync(Encoding.UTF8.GetBytes(msg));
        }

        private static void KeysCommand(Socket socket)
        {
            string[] keys = Storage.Instance.GetAllKeys();
            string msg = Resp.MakeArray(keys);
            Console.WriteLine("Sending all keys from data");
            socket.SendAsync(Encoding.UTF8.GetBytes(msg));
        }
    }
}