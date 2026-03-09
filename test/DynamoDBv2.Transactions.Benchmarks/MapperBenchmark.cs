using Amazon.DynamoDBv2;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using DynamoDBv2.Transactions.Benchmarks.Entities;

namespace DynamoDBv2.Transactions.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.HostProcess, launchCount: 3, warmupCount: 5, iterationCount: 20)]
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

    /// <summary>
    /// Benchmarks with realistic complex entity (~20 properties, nested objects, collections).
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.HostProcess, launchCount: 3, warmupCount: 5, iterationCount: 20)]
    public class ComplexEntityBenchmark
    {
        private OrderEntity _generatedOrder = null!;
        private ReflectionOrderEntity _reflectionOrder = null!;

        [GlobalSetup]
        public void Setup()
        {
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

            _generatedOrder = new OrderEntity
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

            _reflectionOrder = new ReflectionOrderEntity
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

            // Warm up reflection caches
            DynamoDbMapper.MapToAttribute(_reflectionOrder, DynamoDBEntryConversion.V2);
            DynamoDbMapper.GetPropertyAttributedName(typeof(ReflectionOrderEntity), "OrderId");
            DynamoDbMapper.GetHashKeyAttributeName(typeof(ReflectionOrderEntity));
            DynamoDbMapper.GetVersion(_reflectionOrder);
            DynamoDbMapper.GetTableName(typeof(ReflectionOrderEntity));
        }

        // --- MapToAttribute (complex) ---

        [Benchmark(Description = "MapToAttribute complex (source-generated)")]
        public object MapToAttribute_Generated()
        {
            return DynamoDbMapper.MapToAttribute(_generatedOrder, DynamoDBEntryConversion.V2);
        }

        [Benchmark(Baseline = true, Description = "MapToAttribute complex (reflection)")]
        public object MapToAttribute_Reflection()
        {
            return DynamoDbMapper.MapToAttribute(_reflectionOrder, DynamoDBEntryConversion.V2);
        }

        // --- GetPropertyAttributedName (complex) ---

        [Benchmark(Description = "GetPropertyAttributedName complex (source-generated)")]
        public string GetPropertyAttributedName_Generated()
        {
            return DynamoDbMapper.GetPropertyAttributedName(typeof(OrderEntity), "TotalAmount");
        }

        [Benchmark(Description = "GetPropertyAttributedName complex (reflection)")]
        public string GetPropertyAttributedName_Reflection()
        {
            return DynamoDbMapper.GetPropertyAttributedName(typeof(ReflectionOrderEntity), "TotalAmount");
        }

        // --- GetVersion (complex) ---

        [Benchmark(Description = "GetVersion complex (source-generated)")]
        public object GetVersion_Generated()
        {
            return DynamoDbMapper.GetVersion(_generatedOrder);
        }

        [Benchmark(Description = "GetVersion complex (reflection)")]
        public object GetVersion_Reflection()
        {
            return DynamoDbMapper.GetVersion(_reflectionOrder);
        }
    }
}
