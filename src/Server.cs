using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

Console.WriteLine("Logs from your program will appear here!");

await RunServerAsync();

async Task RunServerAsync()
{
	TcpListener server = new TcpListener(IPAddress.Any, 6379);

	server.Start();
	server.AcceptSocket(); // This line will block the program until a client connects

	var clientSocket = server.AcceptSocket();
	await clientSocket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"), SocketFlags.None);
}