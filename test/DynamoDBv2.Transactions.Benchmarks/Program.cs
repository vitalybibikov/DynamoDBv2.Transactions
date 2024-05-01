using BenchmarkDotNet.Running;

namespace DynamoDBv2.Transactions.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
           BenchmarkRunner.Run<Benchmark>();
        }
    }
}