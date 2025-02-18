using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("Logs from your program will appear here!");


TcpListener server = new TcpListener(IPAddress.Any, 6379);

server.Start();
server.AcceptSocket(); // This line will block the program until a client connects

var clientSocket = server.AcceptSocket();
clientSocket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"), SocketFlags.None);