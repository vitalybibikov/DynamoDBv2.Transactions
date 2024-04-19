using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;

namespace DynamoDBv2.Transactions.Benchmarks
{
    [MemoryDiagnoser]
    [Config(typeof(Config))]
    public class Benchmark
    {
        private AwsDBContextProvider _db;
        private BenchmarkTable _benchmarkTable;

        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(Job.MediumRun
                    .WithLaunchCount(1)
                    .WithId("OutOfProc"));

                AddJob(Job.MediumRun
                    .WithLaunchCount(1)
                    .WithToolchain(InProcessEmitToolchain.Instance)
                    .WithId("InProcess"));
            }
        }

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            _db = new AwsDBContextProvider();

            try
            {
                await _db.Client.CreateTableAsync(
                    new CreateTableRequest(nameof(BenchmarkTable),
                        [new(nameof(BenchmarkTable.UserId), KeyType.HASH)],
                        [new(nameof(BenchmarkTable.UserId), ScalarAttributeType.S)],
                        new ProvisionedThroughput { ReadCapacityUnits = 1, WriteCapacityUnits = 1 }));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
     

            var userId1 = Guid.NewGuid().ToString();

            _benchmarkTable = new BenchmarkTable
            {
                UserId = userId1,
                SomeInt = 123456789,
                SomeNullableInt32 = null,
                SomeLong = int.MaxValue,
                SomeNullableLong = null,
                SomeFloat = 123.456f,
                SomeNullableFloat = null,
                SomeDecimal = 123456789.123m,
                SomeNullableDecimal = null,
                SomeDate = DateTime.UtcNow,
                SomeNullableDate1 = null,
                SomeBool = true,
                SomeNullableBool = null,
                SomeClass = new SomeClass { X = "TestX", Y = "TestY" },
                SomeRecord = new SomeRecord { X = "RecordX", Y = "RecordY" },
                SomeClassList = new List<SomeClass> { new SomeClass { X = "ListX", Y = "ListY" } },
                SomeBytes = [1, 2, 3, 4, 5, 6, 7, 8],
                SomeMemoryStream = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8]),
                SomeClassDictionary = new Dictionary<string, SomeClass> { { "Key1", new SomeClass { X = "DictX", Y = "DictY" } } }
            };
        }

        [Benchmark]
        public async Task DbWrapper()
        {
            _benchmarkTable.UserId = Guid.NewGuid().ToString();
            await _db.Context.SaveAsync(_benchmarkTable);
        }
        [Benchmark]
        public async Task CustomWrapper()
        {
            _benchmarkTable.UserId = Guid.NewGuid().ToString();
            await using var writer = new DynamoDbTransactor(new TransactionManager(_db.Client));
            writer.CreateOrUpdate(_benchmarkTable);
        }

        [GlobalCleanup]
        public async Task GlobalCleanup()
        {
            //try
            //{
            //    await _db.Client.DeleteTableAsync(nameof(BenchmarkTable));
            //}
            //catch (Exception e)
            //{
            //    Console.WriteLine(e);
            //}
        }
    }

}
