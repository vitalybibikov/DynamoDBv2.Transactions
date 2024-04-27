using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;

namespace DynamoDBv2.Transactions.Benchmarks
{
    [MemoryDiagnoser]
    [Config(typeof(Config))]
    public class Benchmark
    {
        private AwsDBContextProvider _db;

        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(Job.MediumRun
                    .WithLaunchCount(3)
                    .WithId("OutOfProc"));
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
                        new ProvisionedThroughput { ReadCapacityUnits = 100, WriteCapacityUnits = 100 }));

                await _db.Client.CreateTableAsync(
                    new CreateTableRequest(nameof(BenchmarkTable1),
                        [new(nameof(BenchmarkTable1.UserId), KeyType.HASH)],
                        [new(nameof(BenchmarkTable1.UserId), ScalarAttributeType.S)],
                        new ProvisionedThroughput { ReadCapacityUnits = 100, WriteCapacityUnits = 100 }));
            }
            catch (Exception e)
            {
                //mute
            }
        }

        [Benchmark]
        public async Task DynamoDbTransactionsWrapper()
        {
            var userId1 = Guid.NewGuid().ToString();
            var benchmarkTable = new BenchmarkTable
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
                SomeBytes = [1, 2, 3, 4, 5, 6, 7, 8],
                SomeMemoryStream = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8]),
            };

            await using var writer = new DynamoDbTransactor(_db.Client);
            writer.CreateOrUpdate(benchmarkTable);
        }

        [Benchmark]
        public async Task OriginalWrapper()
        {
            var userId2 = Guid.NewGuid().ToString();
            var benchmarkTable1 = new BenchmarkTable1
            {
                UserId = userId2,
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
                SomeBytes = [1, 2, 3, 4, 5, 6, 7, 8],
                SomeMemoryStream = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8]),
            };

            await _db.Context.SaveAsync(benchmarkTable1, new DynamoDBOperationConfig { Conversion = DynamoDBEntryConversion.V2 });
        }

        [Benchmark]
        public async Task DynamoDbTransactionsWrapper3Items()
        {
            var benchmarkTableItems = new BenchmarkTable[3];

            for (int i = 0; i < benchmarkTableItems.Length; i++)
            {
                var userId = Guid.NewGuid().ToString();

                benchmarkTableItems[i] = new BenchmarkTable
                {
                    UserId = userId,
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
                    SomeBytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 },
                    SomeMemoryStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }),
                };
            }

            await using var writer = new DynamoDbTransactor(_db.Client);

            foreach (var item in benchmarkTableItems)
            {
                writer.CreateOrUpdate(item);
            }
        }

        [Benchmark]
        public async Task OriginalWrapper3Items()
        {
            var benchmarkTableItems = new BenchmarkTable1[3]; 

            for (int i = 0; i < benchmarkTableItems.Length; i++)
            {
                var userId = Guid.NewGuid().ToString();

                benchmarkTableItems[i] = new BenchmarkTable1
                {
                    UserId = userId, 
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
                    SomeBytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 },
                    SomeMemoryStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }),
                };
            }

            foreach (var item in benchmarkTableItems)
            {
                await _db.Context.SaveAsync(item, new DynamoDBOperationConfig { Conversion = DynamoDBEntryConversion.V2 });
            }
        }

        [GlobalCleanup]
        public async Task GlobalCleanup()
        {
        }
    }

}
