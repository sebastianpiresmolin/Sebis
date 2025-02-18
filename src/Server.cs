using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("Logs from your program will appear here!");


TcpListener server = new TcpListener(IPAddress.Any, 6379);

server.Start();

var clientSocket = server.AcceptSocket();
await clientSocket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"), SocketFlags.None);