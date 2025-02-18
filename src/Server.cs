using System.Net;
using System.Net.Sockets;
using System.Text;

TcpListener server = new TcpListener(IPAddress.Any, 6379);

server.Start();
server.AcceptSocket(); // This line will block the program until a client connects

var clientSocket = server.AcceptSocket();
byte[] response = Encoding.UTF8.GetBytes("+PONG\r\n");
clientSocket.Send(response, SocketFlags.None);