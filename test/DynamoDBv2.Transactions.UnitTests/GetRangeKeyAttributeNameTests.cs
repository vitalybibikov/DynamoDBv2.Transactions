using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

public class GetRangeKeyAttributeNameTests
{
    // ──────────────────────────────────────────────
    //  Source-generated path (partial classes)
    // ──────────────────────────────────────────────

    [Fact]
    public void GetRangeKeyAttributeName_TypeWithRangeKey_ReturnsCorrectName()
    {
        // OrderTestEntity has [DynamoDBRangeKey(AttributeName = "sort_key")]
        var result = DynamoDbMapper.GetRangeKeyAttributeName(typeof(OrderTestEntity));
        Assert.Equal("sort_key", result);
    }

    [Fact]
    public void GetRangeKeyAttributeName_TypeWithoutRangeKey_Throws()
    {
        // ProductTestEntity has no range key
        Assert.Throws<ArgumentException>(() =>
            DynamoDbMapper.GetRangeKeyAttributeName(typeof(ProductTestEntity)));
    }

    [Fact]
    public void GetRangeKeyAttributeName_SourceGenerated_OrderTestEntity()
    {
        // Verify the source-generated path returns the right attribute name
        var result = DynamoDbMapper.GetRangeKeyAttributeName(typeof(OrderTestEntity));
        Assert.Equal("sort_key", result);
    }

    [Fact]
    public void GetRangeKeyAttributeName_SourceGenerated_NoRangeKey_Throws()
    {
        // SomeDynamoDbEntity is partial (source-generated) but has no range key
        Assert.Throws<ArgumentException>(() =>
            DynamoDbMapper.GetRangeKeyAttributeName(typeof(SomeDynamoDbEntity)));
    }

    // ──────────────────────────────────────────────
    //  Reflection fallback path (non-partial classes)
    // ──────────────────────────────────────────────

    [Fact]
    public void GetRangeKeyAttributeName_ReflectionFallback_NoRangeKey_Throws()
    {
        // SomeNotAttributedDynamoDbEntity is non-partial (reflection path) with no range key
        Assert.Throws<ArgumentException>(() =>
            DynamoDbMapper.GetRangeKeyAttributeName(typeof(SomeNotAttributedDynamoDbEntity)));
    }

    // ──────────────────────────────────────────────
    //  Cache behavior (call twice, same result)
    // ──────────────────────────────────────────────

    [Fact]
    public void GetRangeKeyAttributeName_CalledTwice_ReturnsSameResult()
    {
        var result1 = DynamoDbMapper.GetRangeKeyAttributeName(typeof(OrderTestEntity));
        var result2 = DynamoDbMapper.GetRangeKeyAttributeName(typeof(OrderTestEntity));

        Assert.Equal(result1, result2);
        Assert.Equal("sort_key", result1);
    }

    [Fact]
    public void GetRangeKeyAttributeName_ThrowsTwice_ConsistentBehavior()
    {
        // Verify the exception is thrown consistently (not cached as success)
        Assert.Throws<ArgumentException>(() =>
            DynamoDbMapper.GetRangeKeyAttributeName(typeof(ProductTestEntity)));
        Assert.Throws<ArgumentException>(() =>
            DynamoDbMapper.GetRangeKeyAttributeName(typeof(ProductTestEntity)));
    }
}
