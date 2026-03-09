using Amazon.DynamoDBv2;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace DynamoDBv2.Transactions.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.Net90, launchCount: 3, warmupCount: 5, iterationCount: 20)]
    public class MapperBenchmark
    {
        private BenchmarkTable _generatedEntity = null!;
        private ReflectionBenchmarkTable _reflectionEntity = null!;

        [GlobalSetup]
        public void Setup()
        {
            _generatedEntity = new BenchmarkTable
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

            _reflectionEntity = new ReflectionBenchmarkTable
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

            // Warm up reflection caches so we benchmark cached reflection, not cold start
            DynamoDbMapper.MapToAttribute(_reflectionEntity, DynamoDBEntryConversion.V2);
            DynamoDbMapper.GetPropertyAttributedName(typeof(ReflectionBenchmarkTable), "UserId");
            DynamoDbMapper.GetHashKeyAttributeName(typeof(ReflectionBenchmarkTable));
            DynamoDbMapper.GetVersion(_reflectionEntity);
            DynamoDbMapper.GetTableName(typeof(ReflectionBenchmarkTable));
        }

        // --- MapToAttribute ---

        [Benchmark(Description = "MapToAttribute (source-generated)")]
        public object MapToAttribute_Generated()
        {
            return DynamoDbMapper.MapToAttribute(_generatedEntity, DynamoDBEntryConversion.V2);
        }

        [Benchmark(Baseline = true, Description = "MapToAttribute (reflection)")]
        public object MapToAttribute_Reflection()
        {
            return DynamoDbMapper.MapToAttribute(_reflectionEntity, DynamoDBEntryConversion.V2);
        }

        // --- GetPropertyAttributedName ---

        [Benchmark(Description = "GetPropertyAttributedName (source-generated)")]
        public string GetPropertyAttributedName_Generated()
        {
            return DynamoDbMapper.GetPropertyAttributedName(typeof(BenchmarkTable), "UserId");
        }

        [Benchmark(Description = "GetPropertyAttributedName (reflection)")]
        public string GetPropertyAttributedName_Reflection()
        {
            return DynamoDbMapper.GetPropertyAttributedName(typeof(ReflectionBenchmarkTable), "UserId");
        }

        // --- GetHashKeyAttributeName ---

        [Benchmark(Description = "GetHashKeyAttributeName (source-generated)")]
        public string GetHashKeyAttributeName_Generated()
        {
            return DynamoDbMapper.GetHashKeyAttributeName(typeof(BenchmarkTable));
        }

        [Benchmark(Description = "GetHashKeyAttributeName (reflection)")]
        public string GetHashKeyAttributeName_Reflection()
        {
            return DynamoDbMapper.GetHashKeyAttributeName(typeof(ReflectionBenchmarkTable));
        }

        // --- GetVersion ---

        [Benchmark(Description = "GetVersion (source-generated)")]
        public object GetVersion_Generated()
        {
            return DynamoDbMapper.GetVersion(_generatedEntity);
        }

        [Benchmark(Description = "GetVersion (reflection)")]
        public object GetVersion_Reflection()
        {
            return DynamoDbMapper.GetVersion(_reflectionEntity);
        }

        // --- GetTableName ---

        [Benchmark(Description = "GetTableName (source-generated)")]
        public string GetTableName_Generated()
        {
            return DynamoDbMapper.GetTableName(typeof(BenchmarkTable));
        }

        [Benchmark(Description = "GetTableName (reflection)")]
        public string GetTableName_Reflection()
        {
            return DynamoDbMapper.GetTableName(typeof(ReflectionBenchmarkTable));
        }
    }
}
