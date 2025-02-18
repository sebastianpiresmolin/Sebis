using codecrafters_redis.src;
using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

internal class Program
{
    private static void Main(string[] args)
    {
        //Config settings
        for (int i = 0; i < args.Length; i += 2)
        {
            Rdb.Instance.SetConfig(args[i].Substring(2), args[i + 1]);
        }
        Rdb.Instance.ReadDb();

        TcpListener server = new TcpListener(IPAddress.Any, 6379);
        server.Start();

        while (true)
        {
            Socket clientSocket = server.AcceptSocket(); // wait for client
            Thread connThread = new Thread(() => { HandleConnection(clientSocket); });
            connThread.Start();
        }
    }

    private static void HandleConnection(Socket socket)
    {
        byte[] buffer = new byte[socket.ReceiveBufferSize];
        while (socket.Connected)
        {
            socket.Receive(buffer);
            string[] commands = Resp.ParseMessage(Encoding.UTF8.GetString(buffer));

            foreach (string command in commands)
            {
                Console.WriteLine(command);
            }

            CommandHandler.HandleCommandArray(socket, commands);
        }
    }
}