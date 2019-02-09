using System;
using Cassandra;

namespace srds_cassandra
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to cassandra powered hotel booking!");
            Backend.Cli cli = new Backend.Cli();
            cli.Run();
        }
    }
}
