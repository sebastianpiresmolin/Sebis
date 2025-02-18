using System.Net;
using System.Net.Sockets;
using System.Text;

Console.WriteLine("Logs from your program will appear here!");


TcpListener server = new TcpListener(IPAddress.Any, 6379);

server.Start();

var clientSocket = server.AcceptSocket();
while (clientSocket.Connected)
{
    byte[] buffer = new byte[1024];
    int bytesRead = await clientSocket.ReceiveAsync(buffer, SocketFlags.None);
    string request = Encoding.UTF8.GetString(buffer, 0, bytesRead);
    Console.WriteLine(request);
    if (request == "PING\r\n")
    {
        await clientSocket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"), SocketFlags.None);
    }
}
