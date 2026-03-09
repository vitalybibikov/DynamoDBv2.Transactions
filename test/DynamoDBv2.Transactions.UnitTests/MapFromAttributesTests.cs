using System.Globalization;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

public class MapFromAttributesTests
{
    // ──────────────────────────────────────────────
    //  Source-generated path — basic deserialization
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_SourceGenerated_DeserializesAllProperties()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "MyId", new AttributeValue { S = "id-1" } },
            { "Name", new AttributeValue { S = "Test" } },
            { "Status", new AttributeValue { S = "Active" } },
            { "Amount", new AttributeValue { N = "42.5" } },
            { "Version", new AttributeValue { N = "3" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<SomeDynamoDbEntity>(attrs);

        Assert.NotNull(entity);
        Assert.Equal("id-1", entity.Id);
        Assert.Equal("Test", entity.Name);
        Assert.Equal("Active", entity.Status);
        Assert.Equal(42.5, entity.Amount);
        Assert.Equal(3L, entity.Version);
    }

    [Fact]
    public void MapFromAttributes_PartialAttributes_SetsOnlyProvidedProperties()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "MyId", new AttributeValue { S = "id-2" } },
            { "Name", new AttributeValue { S = "Partial" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<SomeDynamoDbEntity>(attrs);

        Assert.Equal("id-2", entity.Id);
        Assert.Equal("Partial", entity.Name);
        Assert.Null(entity.Status);
        Assert.Equal(0.0, entity.Amount);
        Assert.Null(entity.Version);
    }

    [Fact]
    public void MapFromAttributes_EmptyDictionary_ReturnsDefaultInstance()
    {
        var attrs = new Dictionary<string, AttributeValue>();

        var entity = DynamoDbMapper.MapFromAttributes<SomeDynamoDbEntity>(attrs);

        Assert.NotNull(entity);
        Assert.Null(entity.Id);
    }

    [Fact]
    public void MapFromAttributes_RoundTrip_MatchesOriginal()
    {
        var original = new SomeDynamoDbEntity
        {
            Id = "roundtrip-1",
            Name = "Round Trip",
            Status = "Completed",
            Amount = 99.99,
            Version = 5
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = DynamoDbMapper.MapFromAttributes<SomeDynamoDbEntity>(attrs);

        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Name, deserialized.Name);
        Assert.Equal(original.Status, deserialized.Status);
        Assert.Equal(original.Amount, deserialized.Amount);
        Assert.Equal(original.Version, deserialized.Version);
    }

    [Fact]
    public void MapFromAttributes_ProductTestEntity_SourceGenerated()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "product_id", new AttributeValue { S = "prod-42" } },
            { "Name", new AttributeValue { S = "Gadget" } },
            { "Price", new AttributeValue { N = "19.99" } },
            { "InStock", new AttributeValue { BOOL = true } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<ProductTestEntity>(attrs);

        Assert.Equal("prod-42", entity.ProductId);
        Assert.Equal("Gadget", entity.Name);
        Assert.Equal(19.99m, entity.Price);
        Assert.True(entity.InStock);
    }

    [Fact]
    public void MapFromAttributes_OrderTestEntity_WithCompositeKey()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "order_id", new AttributeValue { S = "ord-1" } },
            { "sort_key", new AttributeValue { S = "SK#2024" } },
            { "status", new AttributeValue { S = "Pending" } },
            { "total", new AttributeValue { N = "150.75" } },
            { "customer_name", new AttributeValue { S = "Bob" } },
            { "Version", new AttributeValue { N = "1" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<OrderTestEntity>(attrs);

        Assert.Equal("ord-1", entity.OrderId);
        Assert.Equal("SK#2024", entity.SortKey);
        Assert.Equal("Pending", entity.Status);
        Assert.Equal(150.75, entity.Total);
        Assert.Equal("Bob", entity.CustomerName);
        Assert.Equal(1L, entity.Version);
    }

    [Fact]
    public void MapFromAttributes_ReflectionFallback_WorksForNonPartialClasses()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "Id", new AttributeValue { S = "np-1" } },
            { "Name", new AttributeValue { S = "Non-Partial" } }
        };

        var entity = (SomeNotAttributedDynamoDbEntity)DynamoDbMapper.MapFromAttributes(
            typeof(SomeNotAttributedDynamoDbEntity), attrs);

        Assert.Equal("np-1", entity.Id);
        Assert.Equal("Non-Partial", entity.Name);
    }

    // ──────────────────────────────────────────────
    //  Primitive type deserialization — source-gen
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_IntProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t1" } },
            { "IntValue", new AttributeValue { N = "42" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(42, entity.IntValue);
    }

    [Fact]
    public void MapFromAttributes_LongProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t2" } },
            { "LongValue", new AttributeValue { N = "9876543210" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(9876543210L, entity.LongValue);
    }

    [Fact]
    public void MapFromAttributes_DecimalProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t3" } },
            { "DecimalValue", new AttributeValue { N = "123.456" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(123.456m, entity.DecimalValue);
    }

    [Fact]
    public void MapFromAttributes_FloatProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t4" } },
            { "FloatValue", new AttributeValue { N = "3.14" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(3.14f, entity.FloatValue);
    }

    [Fact]
    public void MapFromAttributes_DoubleProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t5" } },
            { "DoubleValue", new AttributeValue { N = "2.718281828" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(2.718281828, entity.DoubleValue);
    }

    [Fact]
    public void MapFromAttributes_BoolProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t6" } },
            { "BoolValue", new AttributeValue { BOOL = true } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.True(entity.BoolValue);
    }

    [Fact]
    public void MapFromAttributes_DateTimeProperty_Deserializes()
    {
        var dt = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t7" } },
            { "DateTimeValue", new AttributeValue { S = "2025-06-15T10:30:00.000Z" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(dt, entity.DateTimeValue);
    }

    [Fact]
    public void MapFromAttributes_GuidProperty_Deserializes()
    {
        var guid = Guid.Parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t8" } },
            { "GuidValue", new AttributeValue { S = "a1b2c3d4-e5f6-7890-abcd-ef1234567890" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(guid, entity.GuidValue);
    }

    [Fact]
    public void MapFromAttributes_CharProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t9" } },
            { "CharValue", new AttributeValue { S = "X" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal('X', entity.CharValue);
    }

    [Fact]
    public void MapFromAttributes_StringProperty_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "t10" } },
            { "StringValue", new AttributeValue { S = "hello world" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal("hello world", entity.StringValue);
    }

    // ──────────────────────────────────────────────
    //  Nullable types with null values
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_NullableInt_MissingAttribute_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n1" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Null(entity.NullableInt);
    }

    [Fact]
    public void MapFromAttributes_NullableLong_MissingAttribute_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n2" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Null(entity.NullableLong);
    }

    [Fact]
    public void MapFromAttributes_NullableDecimal_MissingAttribute_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n3" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Null(entity.NullableDecimal);
    }

    [Fact]
    public void MapFromAttributes_NullableDouble_MissingAttribute_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n4" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Null(entity.NullableDouble);
    }

    [Fact]
    public void MapFromAttributes_NullableBool_MissingAttribute_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n5" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Null(entity.NullableBool);
    }

    [Fact]
    public void MapFromAttributes_NullableDateTime_MissingAttribute_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n6" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Null(entity.NullableDateTime);
    }

    // ──────────────────────────────────────────────
    //  Nullable types with non-null values
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_NullableInt_WithValue_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "nv1" } },
            { "NullableInt", new AttributeValue { N = "77" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(77, entity.NullableInt);
    }

    [Fact]
    public void MapFromAttributes_NullableLong_WithValue_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "nv2" } },
            { "NullableLong", new AttributeValue { N = "123456789012" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(123456789012L, entity.NullableLong);
    }

    [Fact]
    public void MapFromAttributes_NullableDecimal_WithValue_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "nv3" } },
            { "NullableDecimal", new AttributeValue { N = "999.99" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(999.99m, entity.NullableDecimal);
    }

    [Fact]
    public void MapFromAttributes_NullableDouble_WithValue_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "nv4" } },
            { "NullableDouble", new AttributeValue { N = "3.14159" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(3.14159, entity.NullableDouble);
    }

    [Fact]
    public void MapFromAttributes_NullableBool_WithValue_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "nv5" } },
            { "NullableBool", new AttributeValue { BOOL = false } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.False(entity.NullableBool);
    }

    [Fact]
    public void MapFromAttributes_NullableDateTime_WithValue_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "nv6" } },
            { "NullableDateTime", new AttributeValue { S = "2025-12-25T00:00:00.000Z" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.NotNull(entity.NullableDateTime);
        Assert.Equal(new DateTime(2025, 12, 25, 0, 0, 0, DateTimeKind.Utc), entity.NullableDateTime!.Value);
    }

    // ──────────────────────────────────────────────
    //  Version property
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_VersionNull_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "v1" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Null(entity.Version);
    }

    [Fact]
    public void MapFromAttributes_VersionNonNull_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "v2" } },
            { "Version", new AttributeValue { N = "42" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(42L, entity.Version);
    }

    // ──────────────────────────────────────────────
    //  Round-trip for all types
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_AllTypes_RoundTrip_MatchesOriginal()
    {
        var guid = Guid.NewGuid();
        var dt = new DateTime(2025, 3, 15, 14, 30, 45, DateTimeKind.Utc);
        var original = new AllTypesTestEntity
        {
            Id = "rt-all",
            IntValue = 42,
            LongValue = 9876543210L,
            DecimalValue = 123.456m,
            FloatValue = 3.14f,
            DoubleValue = 2.718281828,
            BoolValue = true,
            DateTimeValue = dt,
            GuidValue = guid,
            CharValue = 'Z',
            StringValue = "round-trip",
            NullableInt = 99,
            NullableLong = 1234567890L,
            NullableDecimal = 55.55m,
            NullableDouble = 1.23,
            NullableBool = true,
            NullableDateTime = dt,
            Version = 7
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.IntValue, deserialized.IntValue);
        Assert.Equal(original.LongValue, deserialized.LongValue);
        Assert.Equal(original.DecimalValue, deserialized.DecimalValue);
        Assert.Equal(original.FloatValue, deserialized.FloatValue);
        Assert.Equal(original.DoubleValue, deserialized.DoubleValue);
        Assert.Equal(original.BoolValue, deserialized.BoolValue);
        Assert.Equal(original.DateTimeValue, deserialized.DateTimeValue);
        Assert.Equal(original.GuidValue, deserialized.GuidValue);
        Assert.Equal(original.CharValue, deserialized.CharValue);
        Assert.Equal(original.StringValue, deserialized.StringValue);
        Assert.Equal(original.NullableInt, deserialized.NullableInt);
        Assert.Equal(original.NullableLong, deserialized.NullableLong);
        Assert.Equal(original.NullableDecimal, deserialized.NullableDecimal);
        Assert.Equal(original.NullableDouble, deserialized.NullableDouble);
        Assert.Equal(original.NullableBool, deserialized.NullableBool);
        Assert.Equal(original.NullableDateTime, deserialized.NullableDateTime);
        Assert.Equal(original.Version, deserialized.Version);
    }

    [Fact]
    public void MapFromAttributes_AllTypes_RoundTrip_NullableFieldsNull()
    {
        var original = new AllTypesTestEntity
        {
            Id = "rt-null",
            IntValue = 0,
            LongValue = 0,
            DecimalValue = 0,
            FloatValue = 0,
            DoubleValue = 0,
            BoolValue = false,
            StringValue = "only-required",
            NullableInt = null,
            NullableLong = null,
            NullableDecimal = null,
            NullableDouble = null,
            NullableBool = null,
            NullableDateTime = null,
            Version = null
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.Null(deserialized.NullableInt);
        Assert.Null(deserialized.NullableLong);
        Assert.Null(deserialized.NullableDecimal);
        Assert.Null(deserialized.NullableDouble);
        Assert.Null(deserialized.NullableBool);
        Assert.Null(deserialized.NullableDateTime);
        Assert.Null(deserialized.Version);
    }

    // ──────────────────────────────────────────────
    //  Reflection fallback — all types
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_ReflectionFallback_AllTypes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "rf-1" } },
            { "IntValue", new AttributeValue { N = "10" } },
            { "LongValue", new AttributeValue { N = "20" } },
            { "DecimalValue", new AttributeValue { N = "30.5" } },
            { "FloatValue", new AttributeValue { N = "1.5" } },
            { "DoubleValue", new AttributeValue { N = "2.5" } },
            { "BoolValue", new AttributeValue { BOOL = true } },
            { "DateTimeValue", new AttributeValue { S = "2025-01-01T00:00:00.000Z" } },
            { "GuidValue", new AttributeValue { S = "12345678-1234-1234-1234-123456789abc" } },
            { "CharValue", new AttributeValue { S = "A" } },
            { "StringValue", new AttributeValue { S = "reflected" } },
            { "Version", new AttributeValue { N = "5" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);

        Assert.Equal("rf-1", entity.Id);
        Assert.Equal(10, entity.IntValue);
        Assert.Equal(20L, entity.LongValue);
        Assert.Equal(30.5m, entity.DecimalValue);
        Assert.Equal(1.5f, entity.FloatValue);
        Assert.Equal(2.5, entity.DoubleValue);
        Assert.True(entity.BoolValue);
        Assert.Equal(new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), entity.DateTimeValue);
        Assert.Equal(Guid.Parse("12345678-1234-1234-1234-123456789abc"), entity.GuidValue);
        Assert.Equal('A', entity.CharValue);
        Assert.Equal("reflected", entity.StringValue);
        Assert.Equal(5L, entity.Version);
    }

    [Fact]
    public void MapFromAttributes_ReflectionFallback_EmptyDictionary_ReturnsDefaults()
    {
        var attrs = new Dictionary<string, AttributeValue>();

        var entity = (SomeNotAttributedDynamoDbEntity)DynamoDbMapper.MapFromAttributes(
            typeof(SomeNotAttributedDynamoDbEntity), attrs);

        Assert.NotNull(entity);
        Assert.Null(entity.Id);
        Assert.Null(entity.Name);
        Assert.Equal(0.0, entity.Amount);
        Assert.Null(entity.Version);
    }

    [Fact]
    public void MapFromAttributes_ReflectionFallback_NullableTypes_WithValues()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "rf-nv" } },
            { "NullableInt", new AttributeValue { N = "88" } },
            { "NullableLong", new AttributeValue { N = "999" } },
            { "NullableDecimal", new AttributeValue { N = "12.34" } },
            { "NullableDouble", new AttributeValue { N = "5.67" } },
            { "NullableBool", new AttributeValue { BOOL = false } },
            { "NullableDateTime", new AttributeValue { S = "2025-06-01T12:00:00.000Z" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);

        Assert.Equal(88, entity.NullableInt);
        Assert.Equal(999L, entity.NullableLong);
        Assert.Equal(12.34m, entity.NullableDecimal);
        Assert.Equal(5.67, entity.NullableDouble);
        Assert.False(entity.NullableBool);
        Assert.Equal(new DateTime(2025, 6, 1, 12, 0, 0, DateTimeKind.Utc), entity.NullableDateTime);
    }

    [Fact]
    public void MapFromAttributes_ReflectionFallback_RoundTrip()
    {
        var original = new SomeNotAttributedDynamoDbEntity
        {
            Id = "rt-ref",
            Name = "Reflection RT",
            Status = "Done",
            Amount = 100.5,
            Version = 3
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (SomeNotAttributedDynamoDbEntity)DynamoDbMapper.MapFromAttributes(
            typeof(SomeNotAttributedDynamoDbEntity), attrs);

        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Name, deserialized.Name);
        Assert.Equal(original.Status, deserialized.Status);
        Assert.Equal(original.Amount, deserialized.Amount);
        Assert.Equal(original.Version, deserialized.Version);
    }

    // ──────────────────────────────────────────────
    //  Edge cases
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_UnknownExtraAttributes_AreIgnored()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "MyId", new AttributeValue { S = "extra-1" } },
            { "Name", new AttributeValue { S = "Known" } },
            { "CompletelyUnknownField", new AttributeValue { S = "should be ignored" } },
            { "AnotherExtra", new AttributeValue { N = "999" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<SomeDynamoDbEntity>(attrs);

        Assert.Equal("extra-1", entity.Id);
        Assert.Equal("Known", entity.Name);
    }

    [Fact]
    public void MapFromAttributes_MissingAttributes_PartialProjection()
    {
        // Simulates a projection that only returns some fields
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "order_id", new AttributeValue { S = "partial-1" } },
            { "status", new AttributeValue { S = "Shipped" } }
            // total, customer_name, sort_key, Version all missing
        };

        var entity = DynamoDbMapper.MapFromAttributes<OrderTestEntity>(attrs);

        Assert.Equal("partial-1", entity.OrderId);
        Assert.Equal("Shipped", entity.Status);
        Assert.Equal("", entity.SortKey); // default value
        Assert.Equal(0.0, entity.Total); // default for double
        Assert.Equal("", entity.CustomerName); // default value
        Assert.Null(entity.Version); // nullable stays null
    }

    [Fact]
    public void MapFromAttributes_CultureInvariant_DecimalWithDot()
    {
        // Ensures decimal parsing uses invariant culture (dot as decimal separator)
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "ci-1" } },
            { "DecimalValue", new AttributeValue { N = "1234.56" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(1234.56m, entity.DecimalValue);
    }

    [Fact]
    public void MapFromAttributes_DateTimeUtcRoundTrip_PreservesMilliseconds()
    {
        var dt = new DateTime(2025, 7, 4, 18, 30, 45, 123, DateTimeKind.Utc);
        var formatted = dt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");

        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "dt-ms" } },
            { "DateTimeValue", new AttributeValue { S = formatted } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);
        Assert.Equal(dt.Year, entity.DateTimeValue.Year);
        Assert.Equal(dt.Month, entity.DateTimeValue.Month);
        Assert.Equal(dt.Day, entity.DateTimeValue.Day);
        Assert.Equal(dt.Hour, entity.DateTimeValue.Hour);
        Assert.Equal(dt.Minute, entity.DateTimeValue.Minute);
        Assert.Equal(dt.Second, entity.DateTimeValue.Second);
        Assert.Equal(dt.Millisecond, entity.DateTimeValue.Millisecond);
    }

    [Fact]
    public void MapFromAttributes_BoolFalse_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "product_id", new AttributeValue { S = "p-bool" } },
            { "Name", new AttributeValue { S = "Out of Stock" } },
            { "Price", new AttributeValue { N = "0" } },
            { "InStock", new AttributeValue { BOOL = false } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<ProductTestEntity>(attrs);
        Assert.False(entity.InStock);
    }

    // ──────────────────────────────────────────────
    //  V1 boolean format (N: "1"/"0") compatibility
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_BoolTrue_V1NumericFormat_ReflectionFallback()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "v1-bool-true" } },
            { "BoolValue", new AttributeValue { N = "1" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);
        Assert.True(entity.BoolValue);
    }

    [Fact]
    public void MapFromAttributes_BoolFalse_V1NumericFormat_ReflectionFallback()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "v1-bool-false" } },
            { "BoolValue", new AttributeValue { N = "0" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);
        Assert.False(entity.BoolValue);
    }

    [Fact]
    public void MapFromAttributes_NullableBoolTrue_V1NumericFormat_ReflectionFallback()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "v1-nbool-true" } },
            { "NullableBool", new AttributeValue { N = "1" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);
        Assert.True(entity.NullableBool);
    }

    [Fact]
    public void MapFromAttributes_NullableBoolFalse_V1NumericFormat_ReflectionFallback()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "v1-nbool-false" } },
            { "NullableBool", new AttributeValue { N = "0" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);
        Assert.NotNull(entity.NullableBool);
        Assert.False(entity.NullableBool);
    }

    [Fact]
    public void MapFromAttributes_ReflectionFallback_BoolTrue_V1NumericFormat()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "rf-v1-bool" } },
            { "BoolValue", new AttributeValue { N = "1" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);
        Assert.True(entity.BoolValue);
    }

    [Fact]
    public void MapFromAttributes_ReflectionFallback_BoolFalse_V1NumericFormat()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "rf-v1-boolfalse" } },
            { "BoolValue", new AttributeValue { N = "0" } }
        };

        var entity = (AllTypesReflectionEntity)DynamoDbMapper.MapFromAttributes(
            typeof(AllTypesReflectionEntity), attrs);
        Assert.False(entity.BoolValue);
    }
}
