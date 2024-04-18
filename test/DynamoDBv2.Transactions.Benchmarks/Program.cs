using BenchmarkDotNet.Running;
using DynamoDBv2.Transactions.Benchmarks;

var summary = BenchmarkRunner.Run(typeof(Benchmark));