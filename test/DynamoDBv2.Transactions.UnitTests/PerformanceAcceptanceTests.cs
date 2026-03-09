using System.Diagnostics;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Performance acceptance tests that assert source-generated code is faster
/// than reflection. Uses relative comparisons (ratios) to be stable across
/// different CI runner hardware.
/// </summary>
public class PerformanceAcceptanceTests
{
    private const int Iterations = 5000;
    private const int WarmupIterations = 1000;
    // Conservative threshold — benchmarks show 5x+ but CI runners are noisy
    private const double MinSpeedupRatio = 1.3;

    [Fact]
    public void MapToAttributes_SourceGen_FasterThanReflection()
    {
        // Warmup both paths
        for (int i = 0; i < WarmupIterations; i++)
        {
            DynamoDbMapper.MapToAttribute(CreateSourceGenEntity());
            DynamoDbMapper.MapToAttribute(CreateReflectionEntity());
        }

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            DynamoDbMapper.MapToAttribute(CreateSourceGenEntity());
        }
        sw.Stop();
        var sourceGenMs = sw.Elapsed.TotalMilliseconds;

        sw.Restart();
        for (int i = 0; i < Iterations; i++)
        {
            DynamoDbMapper.MapToAttribute(CreateReflectionEntity());
        }
        sw.Stop();
        var reflectionMs = sw.Elapsed.TotalMilliseconds;

        var ratio = reflectionMs / sourceGenMs;
        Assert.True(ratio >= MinSpeedupRatio,
            $"Source-gen MapToAttributes should be >= {MinSpeedupRatio}x faster. " +
            $"Ratio: {ratio:F2}x (source-gen: {sourceGenMs:F1}ms, reflection: {reflectionMs:F1}ms)");
    }

    [Fact]
    public void MapFromAttributes_SourceGen_FasterThanReflection()
    {
        var attrs = CreateTestAttributes();

        // Warmup
        for (int i = 0; i < WarmupIterations; i++)
        {
            DynamoDbMapper.MapFromAttributes(typeof(AllTypesTestEntity), attrs);
            DynamoDbMapper.MapFromAttributes(typeof(AllTypesReflectionEntity), attrs);
        }

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            DynamoDbMapper.MapFromAttributes(typeof(AllTypesTestEntity), attrs);
        }
        sw.Stop();
        var sourceGenMs = sw.Elapsed.TotalMilliseconds;

        sw.Restart();
        for (int i = 0; i < Iterations; i++)
        {
            DynamoDbMapper.MapFromAttributes(typeof(AllTypesReflectionEntity), attrs);
        }
        sw.Stop();
        var reflectionMs = sw.Elapsed.TotalMilliseconds;

        var ratio = reflectionMs / sourceGenMs;
        Assert.True(ratio >= MinSpeedupRatio,
            $"Source-gen MapFromAttributes should be >= {MinSpeedupRatio}x faster. " +
            $"Ratio: {ratio:F2}x (source-gen: {sourceGenMs:F1}ms, reflection: {reflectionMs:F1}ms)");
    }

    [Fact]
    public void GetTableName_SourceGen_FasterThanReflection()
    {
        // Warmup
        for (int i = 0; i < WarmupIterations; i++)
        {
            DynamoDbMapper.GetTableName(typeof(AllTypesTestEntity));
            DynamoDbMapper.GetTableName(typeof(AllTypesReflectionEntity));
        }

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations * 10; i++)
        {
            DynamoDbMapper.GetTableName(typeof(AllTypesTestEntity));
        }
        sw.Stop();
        var sourceGenMs = sw.Elapsed.TotalMilliseconds;

        sw.Restart();
        for (int i = 0; i < Iterations * 10; i++)
        {
            DynamoDbMapper.GetTableName(typeof(AllTypesReflectionEntity));
        }
        sw.Stop();
        var reflectionMs = sw.Elapsed.TotalMilliseconds;

        var ratio = reflectionMs / sourceGenMs;
        // GetTableName has a large gap (60x in benchmarks) but CI can be noisy
        Assert.True(ratio >= 1.2,
            $"Source-gen GetTableName should be faster. " +
            $"Ratio: {ratio:F2}x (source-gen: {sourceGenMs:F1}ms, reflection: {reflectionMs:F1}ms)");
    }

    [Fact]
    public void RoundTrip_SourceGen_FasterThanReflection()
    {
        var attrs = CreateTestAttributes();

        // Warmup
        for (int i = 0; i < WarmupIterations; i++)
        {
            var obj1 = DynamoDbMapper.MapFromAttributes(typeof(AllTypesTestEntity), attrs);
            DynamoDbMapper.MapToAttribute(obj1);

            var obj2 = DynamoDbMapper.MapFromAttributes(typeof(AllTypesReflectionEntity), attrs);
            DynamoDbMapper.MapToAttribute(obj2);
        }

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            var obj = DynamoDbMapper.MapFromAttributes(typeof(AllTypesTestEntity), attrs);
            DynamoDbMapper.MapToAttribute(obj);
        }
        sw.Stop();
        var sourceGenMs = sw.Elapsed.TotalMilliseconds;

        sw.Restart();
        for (int i = 0; i < Iterations; i++)
        {
            var obj = DynamoDbMapper.MapFromAttributes(typeof(AllTypesReflectionEntity), attrs);
            DynamoDbMapper.MapToAttribute(obj);
        }
        sw.Stop();
        var reflectionMs = sw.Elapsed.TotalMilliseconds;

        var ratio = reflectionMs / sourceGenMs;
        Assert.True(ratio >= MinSpeedupRatio,
            $"Source-gen round-trip should be >= {MinSpeedupRatio}x faster. " +
            $"Ratio: {ratio:F2}x (source-gen: {sourceGenMs:F1}ms, reflection: {reflectionMs:F1}ms)");
    }

    [Fact]
    public void MapToAttributes_AbsoluteTime_UnderThreshold()
    {
        // Warmup
        for (int i = 0; i < WarmupIterations; i++)
        {
            DynamoDbMapper.MapToAttribute(CreateSourceGenEntity());
        }

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            DynamoDbMapper.MapToAttribute(CreateSourceGenEntity());
        }
        sw.Stop();

        var avgMicroseconds = (sw.Elapsed.TotalMilliseconds * 1000.0) / Iterations;

        // Source-gen should complete under 100 microseconds per call on any modern hardware
        Assert.True(avgMicroseconds < 100,
            $"Source-gen MapToAttributes averaged {avgMicroseconds:F1}us per call — exceeds 100us threshold");
    }

    private static AllTypesTestEntity CreateSourceGenEntity() => new()
    {
        Id = "perf-test-001",
        IntValue = 42,
        LongValue = 123456789L,
        DecimalValue = 99.99m,
        FloatValue = 3.14f,
        DoubleValue = 2.718,
        BoolValue = true,
        DateTimeValue = DateTime.UtcNow,
        GuidValue = Guid.NewGuid(),
        CharValue = 'X',
        StringValue = "Performance test string",
        NullableInt = 7,
        NullableLong = 999L,
        NullableDecimal = 1.23m,
        NullableDouble = 4.56,
        NullableBool = false,
        NullableDateTime = DateTime.UtcNow
    };

    private static AllTypesReflectionEntity CreateReflectionEntity() => new()
    {
        Id = "perf-test-001",
        IntValue = 42,
        LongValue = 123456789L,
        DecimalValue = 99.99m,
        FloatValue = 3.14f,
        DoubleValue = 2.718,
        BoolValue = true,
        DateTimeValue = DateTime.UtcNow,
        GuidValue = Guid.NewGuid(),
        CharValue = 'X',
        StringValue = "Performance test string",
        NullableInt = 7,
        NullableLong = 999L,
        NullableDecimal = 1.23m,
        NullableDouble = 4.56,
        NullableBool = false,
        NullableDateTime = DateTime.UtcNow
    };

    private static Dictionary<string, AttributeValue> CreateTestAttributes() => new()
    {
        ["pk"] = new AttributeValue { S = "perf-test-001" },
        ["IntValue"] = new AttributeValue { N = "42" },
        ["LongValue"] = new AttributeValue { N = "123456789" },
        ["DecimalValue"] = new AttributeValue { N = "99.99" },
        ["FloatValue"] = new AttributeValue { N = "3.14" },
        ["DoubleValue"] = new AttributeValue { N = "2.718" },
        ["BoolValue"] = new AttributeValue { BOOL = true },
        ["DateTimeValue"] = new AttributeValue { S = "2025-06-15T10:30:00.000Z" },
        ["GuidValue"] = new AttributeValue { S = Guid.NewGuid().ToString() },
        ["CharValue"] = new AttributeValue { S = "X" },
        ["StringValue"] = new AttributeValue { S = "Performance test string" },
        ["NullableInt"] = new AttributeValue { N = "7" },
        ["NullableLong"] = new AttributeValue { N = "999" },
        ["NullableDecimal"] = new AttributeValue { N = "1.23" },
        ["NullableDouble"] = new AttributeValue { N = "4.56" },
        ["NullableBool"] = new AttributeValue { BOOL = false },
        ["NullableDateTime"] = new AttributeValue { S = "2025-06-15T10:30:00.000Z" }
    };
}
