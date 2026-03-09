using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using DynamoDBv2.Transactions.Benchmarks.Entities;

namespace DynamoDBv2.Transactions.Benchmarks
{
    /// <summary>
    /// Benchmarks for MapFromAttributes (deserialization) — source-generated vs reflection.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.HostProcess, launchCount: 3, warmupCount: 5, iterationCount: 20)]
    public class DeserializationBenchmark
    {
        private Dictionary<string, AttributeValue> _generatedAttrs = null!;
        private Dictionary<string, AttributeValue> _reflectionAttrs = null!;
        private Dictionary<string, AttributeValue> _generatedOrderAttrs = null!;
        private Dictionary<string, AttributeValue> _reflectionOrderAttrs = null!;

        // Keep original entities for round-trip serialization step
        private BenchmarkTable _generatedEntity = null!;
        private ReflectionBenchmarkTable _reflectionEntity = null!;

        [GlobalSetup]
        public void Setup()
        {
            // --- Simple entities ---

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

            // Serialize to create attribute dictionaries for deserialization benchmarks
            _generatedAttrs = DynamoDbMapper.MapToAttribute(_generatedEntity, DynamoDBEntryConversion.V2);
            _reflectionAttrs = DynamoDbMapper.MapToAttribute(_reflectionEntity, DynamoDBEntryConversion.V2);

            // --- Complex entities (OrderEntity) ---

            var address = new Address
            {
                Street = "123 Main St",
                City = "Berlin",
                Country = "Germany",
                PostalCode = "10115"
            };

            var items = new List<OrderItem>
            {
                new() { SKU = "SKU-001", ProductName = "Widget A", Quantity = 3, UnitPrice = 29.99m, Discount = 2.50m },
                new() { SKU = "SKU-002", ProductName = "Widget B", Quantity = 1, UnitPrice = 149.00m, Discount = null },
                new() { SKU = "SKU-003", ProductName = "Gadget C", Quantity = 5, UnitPrice = 9.95m, Discount = 1.00m },
            };

            var metadata = new Dictionary<string, string>
            {
                ["source"] = "web",
                ["campaign"] = "summer-sale",
                ["referrer"] = "google",
                ["coupon"] = "SAVE10"
            };

            var generatedOrder = new OrderEntity
            {
                OrderId = "ORD-2026-00001",
                CustomerName = "Max Mustermann",
                Email = "max@example.com",
                Status = "Shipped",
                ShippingMethod = "DHL Express",
                TrackingNumber = "DE123456789",
                ShippingAddress = address,
                OrderItems = items,
                Metadata = metadata,
                SubTotal = 288.72m,
                TaxAmount = 54.86m,
                ShippingCost = 12.99m,
                TotalAmount = 356.57m,
                OrderedAt = new DateTime(2026, 3, 1, 10, 30, 0, DateTimeKind.Utc),
                ShippedAt = new DateTime(2026, 3, 3, 14, 0, 0, DateTimeKind.Utc),
                DeliveredAt = null,
                IsPriority = true,
                RequiresSignature = false,
                Version = 3
            };

            var reflectionOrder = new ReflectionOrderEntity
            {
                OrderId = "ORD-2026-00001",
                CustomerName = "Max Mustermann",
                Email = "max@example.com",
                Status = "Shipped",
                ShippingMethod = "DHL Express",
                TrackingNumber = "DE123456789",
                ShippingAddress = address,
                OrderItems = items,
                Metadata = metadata,
                SubTotal = 288.72m,
                TaxAmount = 54.86m,
                ShippingCost = 12.99m,
                TotalAmount = 356.57m,
                OrderedAt = new DateTime(2026, 3, 1, 10, 30, 0, DateTimeKind.Utc),
                ShippedAt = new DateTime(2026, 3, 3, 14, 0, 0, DateTimeKind.Utc),
                DeliveredAt = null,
                IsPriority = true,
                RequiresSignature = false,
                Version = 3
            };

            _generatedOrderAttrs = DynamoDbMapper.MapToAttribute(generatedOrder, DynamoDBEntryConversion.V2);
            _reflectionOrderAttrs = DynamoDbMapper.MapToAttribute(reflectionOrder, DynamoDBEntryConversion.V2);

            // Warm up reflection caches so we benchmark cached reflection, not cold start
            DynamoDbMapper.MapFromAttributes(typeof(ReflectionBenchmarkTable), _reflectionAttrs);
            DynamoDbMapper.MapFromAttributes(typeof(ReflectionOrderEntity), _reflectionOrderAttrs);
        }

        // --- MapFromAttributes (simple entity) ---

        [Benchmark(Description = "MapFromAttributes (source-generated)")]
        public object MapFromAttributes_Generated()
        {
            return DynamoDbMapper.MapFromAttributes<BenchmarkTable>(_generatedAttrs);
        }

        [Benchmark(Baseline = true, Description = "MapFromAttributes (reflection)")]
        public object MapFromAttributes_Reflection()
        {
            return DynamoDbMapper.MapFromAttributes<ReflectionBenchmarkTable>(_reflectionAttrs);
        }

        // --- MapFromAttributes (complex entity) ---

        [Benchmark(Description = "MapFromAttributes complex (source-generated)")]
        public object MapFromAttributes_ComplexEntity_Generated()
        {
            return DynamoDbMapper.MapFromAttributes<OrderEntity>(_generatedOrderAttrs);
        }

        [Benchmark(Description = "MapFromAttributes complex (reflection)")]
        public object MapFromAttributes_ComplexEntity_Reflection()
        {
            return DynamoDbMapper.MapFromAttributes<ReflectionOrderEntity>(_reflectionOrderAttrs);
        }

        // --- Round-trip: serialize then deserialize ---

        [Benchmark(Description = "RoundTrip (source-generated)")]
        public object RoundTrip_Generated()
        {
            var attrs = DynamoDbMapper.MapToAttribute(_generatedEntity, DynamoDBEntryConversion.V2);
            return DynamoDbMapper.MapFromAttributes<BenchmarkTable>(attrs);
        }

        [Benchmark(Description = "RoundTrip (reflection)")]
        public object RoundTrip_Reflection()
        {
            var attrs = DynamoDbMapper.MapToAttribute(_reflectionEntity, DynamoDBEntryConversion.V2);
            return DynamoDbMapper.MapFromAttributes<ReflectionBenchmarkTable>(attrs);
        }
    }
}
