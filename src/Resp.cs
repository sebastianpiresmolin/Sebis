using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace codecrafters_redis.src
{
    internal class Resp
    {
        public static string[] ParseMessage(string data)
        {
            List<string> result = new List<string>();
            //Console.WriteLine(data);
            int n = int.Parse(data.Substring(1, data.IndexOf("\r\n") - 1));

            //Console.WriteLine(n);
            data = data.Substring(data.IndexOf("\r\n") + 2);
            //Console.WriteLine(data);

            for (int i = 0; i < n; i++)
            {
                result.Add(ParseBulkString(ref data));
            }
            return result.ToArray();
        }

        private static string ParseBulkString(ref string data)
        {
            //Console.WriteLine(data);
            int n = int.Parse(data.Substring(1, data.IndexOf("\r\n") - 1));
            data = data.Substring(data.IndexOf("\r\n") + 2);
            //Console.WriteLine(data);
            string text = data.Substring(0, n);
            data = data.Substring(data.IndexOf("\r\n") + 2);
            //Console.WriteLine(data);

            return text;
        }

        public static string MakeBulkString(string data)
        {
            return $"${data.ToString()?.Length}\r\n{data.ToString()}\r\n";
        }

        public static string MakeNullBulkString()
        {
            return "$-1\r\n";
        }

        public static string MakeSimpleString(string data)
        {
            return $"+{data.ToString()}\r\n";
        }

        public static string MakeArray(string[] data)
        {
            string msg = $"*{data.Length}\r\n";
            foreach (string str in data)
            {
                msg += MakeBulkString(str);
            }
            return msg;
        }
    }
}