using Amazon.DynamoDBv2;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace DynamoDBv2.Transactions.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.Net90, launchCount: 3, warmupCount: 5, iterationCount: 20)]
    public class MapperBenchmark
    {
        private BenchmarkTable _entity = null!;

        [GlobalSetup]
        public void Setup()
        {
            _entity = new BenchmarkTable
            {
                UserId = "bench-user-123",
                SomeInt = 123456789,
                SomeNullableInt32 = 42,
                SomeLong = int.MaxValue,
                SomeNullableLong = 100,
                SomeFloat = 123.456f,
                SomeNullableFloat = 99.9f,
                SomeDecimal = 123456789.123m,
                SomeNullableDecimal = 55.5m,
                SomeDate = DateTime.UtcNow,
                SomeNullableDate1 = DateTime.UtcNow,
                SomeBool = true,
                SomeNullableBool = false,
                SomeBytes = [1, 2, 3, 4, 5, 6, 7, 8],
                SomeMemoryStream = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8]),
                Version = 5
            };
        }

        [Benchmark(Description = "MapToAttribute (source-generated)")]
        public object MapToAttribute_Generated()
        {
            return DynamoDbMapper.MapToAttribute(_entity, DynamoDBEntryConversion.V2);
        }

        [Benchmark(Description = "GetPropertyAttributedName (source-generated)")]
        public string GetPropertyAttributedName_Generated()
        {
            return DynamoDbMapper.GetPropertyAttributedName(typeof(BenchmarkTable), "UserId");
        }

        [Benchmark(Description = "GetHashKeyAttributeName (source-generated)")]
        public string GetHashKeyAttributeName_Generated()
        {
            return DynamoDbMapper.GetHashKeyAttributeName(typeof(BenchmarkTable));
        }

        [Benchmark(Description = "GetVersion (source-generated)")]
        public object GetVersion_Generated()
        {
            return DynamoDbMapper.GetVersion(_entity);
        }

        [Benchmark(Description = "GetTableName (source-generated)")]
        public string GetTableName_Generated()
        {
            return DynamoDbMapper.GetTableName(typeof(BenchmarkTable));
        }
    }
}
