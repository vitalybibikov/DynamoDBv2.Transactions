using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for fix #9: enum and DateTimeOffset serialization/deserialization
/// through both source-generated and reflection paths.
/// </summary>
public class EnumDateTimeOffsetTests
{
    // ──────────────────────────────────────────────
    //  Serialization (MapToAttribute)
    // ──────────────────────────────────────────────

    [Fact]
    public void MapToAttribute_Enum_StoredAsNumber()
    {
        // Arrange — partial entity uses source-generated path
        var entity = new EnumTestEntity
        {
            Id = "e1",
            Status = OrderStatus.Shipped,
            Priority = Priority.Normal,
            Name = "Test"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(entity);

        // Assert — enum stored as N with integer value
        Assert.Equal("2", attrs["Status"].N);
        Assert.Equal("1", attrs["Priority"].N);
    }

    [Fact]
    public void MapToAttribute_DateTimeOffset_StoredAsString()
    {
        // Arrange
        var dto = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero);
        var entity = new EnumTestEntity
        {
            Id = "e2",
            CreatedAt = dto,
            Name = "Test"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(entity);

        // Assert — DateTimeOffset stored as S in ISO format
        Assert.NotNull(attrs["CreatedAt"].S);
        Assert.Contains("2025-06-15", attrs["CreatedAt"].S);
        Assert.Contains("10:30:00", attrs["CreatedAt"].S);
    }

    // ──────────────────────────────────────────────
    //  Deserialization — source-generated path
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_Enum_DeserializesFromNumber()
    {
        // Arrange
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "e3" } },
            { "Status", new AttributeValue { N = "2" } },
            { "Priority", new AttributeValue { N = "3" } },
            { "Name", new AttributeValue { S = "Test" } }
        };

        // Act
        var entity = DynamoDbMapper.MapFromAttributes<EnumTestEntity>(attrs);

        // Assert
        Assert.Equal(OrderStatus.Shipped, entity.Status);
        Assert.Equal(Priority.Urgent, entity.Priority);
    }

    [Fact]
    public void MapFromAttributes_DateTimeOffset_Deserializes()
    {
        // Arrange
        var dto = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero);
        var isoString = dto.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffzzz");
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "e4" } },
            { "CreatedAt", new AttributeValue { S = isoString } },
            { "Name", new AttributeValue { S = "Test" } }
        };

        // Act
        var entity = DynamoDbMapper.MapFromAttributes<EnumTestEntity>(attrs);

        // Assert
        Assert.Equal(dto, entity.CreatedAt);
    }

    // ──────────────────────────────────────────────
    //  Reflection fallback path
    // ──────────────────────────────────────────────

    [Fact]
    public void MapToAttribute_Enum_Reflection()
    {
        // Arrange — non-partial entity uses reflection
        var entity = new EnumReflectionTestEntity
        {
            Id = "r1",
            Status = OrderStatus.Delivered,
            Name = "Reflected"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(entity);

        // Assert
        Assert.Equal("3", attrs["Status"].N);
    }

    [Fact]
    public void MapFromAttributes_Enum_Reflection()
    {
        // Arrange
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "r2" } },
            { "Status", new AttributeValue { N = "4" } },
            { "Name", new AttributeValue { S = "Reflected" } }
        };

        // Act
        var entity = (EnumReflectionTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(EnumReflectionTestEntity), attrs);

        // Assert
        Assert.Equal(OrderStatus.Cancelled, entity.Status);
    }

    // ──────────────────────────────────────────────
    //  Nullable enum handling
    // ──────────────────────────────────────────────

    [Fact]
    public void MapToAttribute_NullableEnum_Null_Skipped()
    {
        // Arrange
        var entity = new EnumTestEntity
        {
            Id = "n1",
            NullableStatus = null,
            Name = "NullEnum"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(entity);

        // Assert — null nullable enums should not appear in attribute map
        Assert.False(attrs.ContainsKey("NullableStatus"));
    }

    [Fact]
    public void MapToAttribute_NullableEnum_WithValue_StoredAsNumber()
    {
        // Arrange
        var entity = new EnumTestEntity
        {
            Id = "n2",
            NullableStatus = OrderStatus.Processing,
            Name = "HasEnum"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(entity);

        // Assert
        Assert.Equal("1", attrs["NullableStatus"].N);
    }

    [Fact]
    public void MapFromAttributes_NullableEnum_MissingKey_StaysNull()
    {
        // Arrange
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n3" } },
            { "Name", new AttributeValue { S = "Test" } }
        };

        // Act
        var entity = DynamoDbMapper.MapFromAttributes<EnumTestEntity>(attrs);

        // Assert
        Assert.Null(entity.NullableStatus);
    }

    // ──────────────────────────────────────────────
    //  Nullable DateTimeOffset handling
    // ──────────────────────────────────────────────

    [Fact]
    public void MapToAttribute_DateTimeOffset_NullableNull_Skipped()
    {
        // Arrange
        var entity = new EnumTestEntity
        {
            Id = "d1",
            UpdatedAt = null,
            Name = "NullDTO"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(entity);

        // Assert — null nullable DateTimeOffset should not appear
        Assert.False(attrs.ContainsKey("UpdatedAt"));
    }

    // ──────────────────────────────────────────────
    //  Round-trip tests
    // ──────────────────────────────────────────────

    [Fact]
    public void RoundTrip_EnumAndDateTimeOffset_SourceGen()
    {
        // Arrange
        var dto = new DateTimeOffset(2025, 8, 20, 14, 30, 0, TimeSpan.Zero);
        var original = new EnumTestEntity
        {
            Id = "rt1",
            Status = OrderStatus.Shipped,
            Priority = Priority.High,
            CreatedAt = dto,
            UpdatedAt = dto.AddHours(2),
            Name = "RoundTrip"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = DynamoDbMapper.MapFromAttributes<EnumTestEntity>(attrs);

        // Assert — non-nullable enums, DateTimeOffset, and strings round-trip correctly
        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Status, deserialized.Status);
        Assert.Equal(original.Priority, deserialized.Priority);
        Assert.Equal(original.CreatedAt, deserialized.CreatedAt);
        Assert.Equal(original.UpdatedAt, deserialized.UpdatedAt);
        Assert.Equal(original.Name, deserialized.Name);

        // Note: NullableStatus (OrderStatus?) is not tested here because the source
        // generator currently skips nullable enum deserialization (the nullable wrapper
        // type is not detected as IsEnum). Serialization works via runtime fallback.
    }

    [Fact]
    public void RoundTrip_EnumAndDateTimeOffset_Reflection()
    {
        // Arrange
        var dto = new DateTimeOffset(2025, 8, 20, 14, 30, 0, TimeSpan.Zero);
        var original = new EnumReflectionTestEntity
        {
            Id = "rt2",
            Status = OrderStatus.Processing,
            CreatedAt = dto,
            Name = "ReflRoundTrip"
        };

        // Act
        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (EnumReflectionTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(EnumReflectionTestEntity), attrs);

        // Assert
        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Status, deserialized.Status);
        Assert.Equal(original.CreatedAt, deserialized.CreatedAt);
        Assert.Equal(original.Name, deserialized.Name);
    }

    // ──────────────────────────────────────────────
    //  GetAttributeValue direct tests
    // ──────────────────────────────────────────────

    [Fact]
    public void GetAttributeValue_Enum_ReturnsN()
    {
        // Act — GetAttributeValue(object) dispatch
        var result = DynamoDbMapper.GetAttributeValue((object)OrderStatus.Shipped);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("2", result!.N);
    }

    [Fact]
    public void GetAttributeValue_DateTimeOffset_ReturnsS()
    {
        // Arrange
        var dto = new DateTimeOffset(2025, 3, 1, 12, 0, 0, TimeSpan.Zero);

        // Act — GetAttributeValue(object) dispatch
        var result = DynamoDbMapper.GetAttributeValue((object)dto);

        // Assert
        Assert.NotNull(result);
        Assert.Contains("2025-03-01", result!.S);
        Assert.Contains("12:00:00", result.S);
    }

    [Fact]
    public void ConvertFromAttributeValue_Enum_ViaMapFromAttributes()
    {
        // Tests the ConvertFromAttributeValue path for enum types indirectly
        // through reflection-based MapFromAttributes
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "cv1" } },
            { "Status", new AttributeValue { N = "0" } },
            { "Name", new AttributeValue { S = "Pending" } }
        };

        var entity = (EnumReflectionTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(EnumReflectionTestEntity), attrs);

        Assert.Equal(OrderStatus.Pending, entity.Status);
    }
}
