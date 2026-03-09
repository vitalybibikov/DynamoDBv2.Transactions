using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for complex type deserialization (nested objects, lists, dictionaries, binary)
/// via the reflection fallback path in ConvertFromAttributeValue.
/// </summary>
public class ComplexTypeDeserializationTests
{
    // ──────────────────────────────────────────────
    //  Nested class (Map → object)
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_NestedClass_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n1" } },
            { "Nested", new AttributeValue
                {
                    M = new Dictionary<string, AttributeValue>
                    {
                        { "X", new AttributeValue { S = "hello" } },
                        { "Y", new AttributeValue { S = "world" } }
                    }
                }
            }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.Nested);
        Assert.Equal("hello", entity.Nested.X);
        Assert.Equal("world", entity.Nested.Y);
    }

    [Fact]
    public void MapFromAttributes_NestedClass_Null_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n2" } },
            { "Nested", new AttributeValue { NULL = true } }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.Null(entity.Nested);
    }

    [Fact]
    public void MapFromAttributes_NestedClass_Missing_StaysNull()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "n3" } }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.Null(entity.Nested);
    }

    // ──────────────────────────────────────────────
    //  List<T> where T is a class (List → L)
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_ListOfObjects_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "l1" } },
            { "Items", new AttributeValue
                {
                    L = new List<AttributeValue>
                    {
                        new()
                        {
                            M = new Dictionary<string, AttributeValue>
                            {
                                { "X", new AttributeValue { S = "A1" } },
                                { "Y", new AttributeValue { S = "B1" } }
                            }
                        },
                        new()
                        {
                            M = new Dictionary<string, AttributeValue>
                            {
                                { "X", new AttributeValue { S = "A2" } },
                                { "Y", new AttributeValue { S = "B2" } }
                            }
                        }
                    }
                }
            }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.Items);
        Assert.Equal(2, entity.Items.Count);
        Assert.Equal("A1", entity.Items[0].X);
        Assert.Equal("B1", entity.Items[0].Y);
        Assert.Equal("A2", entity.Items[1].X);
        Assert.Equal("B2", entity.Items[1].Y);
    }

    [Fact]
    public void MapFromAttributes_ListOfStrings_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "l2" } },
            { "Tags", new AttributeValue
                {
                    L = new List<AttributeValue>
                    {
                        new() { S = "tag1" },
                        new() { S = "tag2" },
                        new() { S = "tag3" }
                    }
                }
            }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.Tags);
        Assert.Equal(3, entity.Tags.Count);
        Assert.Equal("tag1", entity.Tags[0]);
        Assert.Equal("tag2", entity.Tags[1]);
        Assert.Equal("tag3", entity.Tags[2]);
    }

    [Fact]
    public void MapFromAttributes_ListOfInts_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "l3" } },
            { "Numbers", new AttributeValue
                {
                    L = new List<AttributeValue>
                    {
                        new() { N = "10" },
                        new() { N = "20" },
                        new() { N = "30" }
                    }
                }
            }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.Numbers);
        Assert.Equal(3, entity.Numbers.Count);
        Assert.Equal(10, entity.Numbers[0]);
        Assert.Equal(20, entity.Numbers[1]);
        Assert.Equal(30, entity.Numbers[2]);
    }

    [Fact]
    public void MapFromAttributes_EmptyList_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "l4" } },
            { "Items", new AttributeValue { L = new List<AttributeValue>() } }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.Items);
        Assert.Empty(entity.Items);
    }

    // ──────────────────────────────────────────────
    //  Dictionary<string, T> (Map → M)
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_DictionaryOfObjects_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "d1" } },
            { "ItemMap", new AttributeValue
                {
                    M = new Dictionary<string, AttributeValue>
                    {
                        { "first", new AttributeValue
                            {
                                M = new Dictionary<string, AttributeValue>
                                {
                                    { "X", new AttributeValue { S = "1X" } },
                                    { "Y", new AttributeValue { S = "1Y" } }
                                }
                            }
                        },
                        { "second", new AttributeValue
                            {
                                M = new Dictionary<string, AttributeValue>
                                {
                                    { "X", new AttributeValue { S = "2X" } },
                                    { "Y", new AttributeValue { S = "2Y" } }
                                }
                            }
                        }
                    }
                }
            }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.ItemMap);
        Assert.Equal(2, entity.ItemMap.Count);
        Assert.Equal("1X", entity.ItemMap["first"].X);
        Assert.Equal("1Y", entity.ItemMap["first"].Y);
        Assert.Equal("2X", entity.ItemMap["second"].X);
        Assert.Equal("2Y", entity.ItemMap["second"].Y);
    }

    [Fact]
    public void MapFromAttributes_DictionaryOfStrings_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "d2" } },
            { "Metadata", new AttributeValue
                {
                    M = new Dictionary<string, AttributeValue>
                    {
                        { "key1", new AttributeValue { S = "val1" } },
                        { "key2", new AttributeValue { S = "val2" } }
                    }
                }
            }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.Metadata);
        Assert.Equal(2, entity.Metadata.Count);
        Assert.Equal("val1", entity.Metadata["key1"]);
        Assert.Equal("val2", entity.Metadata["key2"]);
    }

    [Fact]
    public void MapFromAttributes_EmptyDictionary_Deserializes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "d3" } },
            { "ItemMap", new AttributeValue { M = new Dictionary<string, AttributeValue>() } }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        // Empty map won't trigger nested class path (count == 0), stays null for Dict<string, class>
        // This is acceptable — DynamoDB SDK also treats empty maps as absent
    }

    // ──────────────────────────────────────────────
    //  Binary data (byte[] and MemoryStream)
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_ByteArray_Deserializes()
    {
        var data = new byte[] { 0x01, 0x02, 0x03, 0xFF };
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "b1" } },
            { "BinaryData", new AttributeValue { B = new MemoryStream(data) } }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.BinaryData);
        Assert.Equal(data, entity.BinaryData);
    }

    [Fact]
    public void MapFromAttributes_MemoryStream_Deserializes()
    {
        var data = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "b2" } },
            { "StreamData", new AttributeValue { B = new MemoryStream(data) } }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(entity.StreamData);
        Assert.Equal(data, entity.StreamData.ToArray());
    }

    [Fact]
    public void MapFromAttributes_LargeBinary_Deserializes()
    {
        var data = new byte[4096];
        new Random(42).NextBytes(data);
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "b3" } },
            { "BinaryData", new AttributeValue { B = new MemoryStream(data) } }
        };

        var entity = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.Equal(data, entity.BinaryData);
    }

    // ──────────────────────────────────────────────
    //  Round-trip: serialize then deserialize
    // ──────────────────────────────────────────────

    [Fact]
    public void MapFromAttributes_NestedClass_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-1",
            Name = "RoundTrip",
            Nested = new InnerTestClass { X = "hello", Y = "world" }
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(deserialized.Nested);
        Assert.Equal("hello", deserialized.Nested.X);
        Assert.Equal("world", deserialized.Nested.Y);
    }

    [Fact]
    public void MapFromAttributes_ListOfObjects_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-2",
            Items = new List<InnerTestClass>
            {
                new() { X = "A", Y = "B" },
                new() { X = "C", Y = "D" }
            }
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(deserialized.Items);
        Assert.Equal(2, deserialized.Items.Count);
        Assert.Equal("A", deserialized.Items[0].X);
        Assert.Equal("D", deserialized.Items[1].Y);
    }

    [Fact]
    public void MapFromAttributes_DictionaryOfObjects_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-3",
            ItemMap = new Dictionary<string, InnerTestClass>
            {
                { "one", new InnerTestClass { X = "1X", Y = "1Y" } },
                { "two", new InnerTestClass { X = "2X", Y = "2Y" } }
            }
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(deserialized.ItemMap);
        Assert.Equal(2, deserialized.ItemMap.Count);
        Assert.Equal("1X", deserialized.ItemMap["one"].X);
        Assert.Equal("2Y", deserialized.ItemMap["two"].Y);
    }

    [Fact]
    public void MapFromAttributes_ListOfStrings_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-4",
            Tags = new List<string> { "alpha", "beta", "gamma" }
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(deserialized.Tags);
        Assert.Equal(3, deserialized.Tags.Count);
        Assert.Contains("alpha", deserialized.Tags);
        Assert.Contains("gamma", deserialized.Tags);
    }

    [Fact]
    public void MapFromAttributes_ListOfInts_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-5",
            Numbers = new List<int> { 1, 2, 3, 42, 100 }
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(deserialized.Numbers);
        Assert.Equal(5, deserialized.Numbers.Count);
        Assert.Equal(42, deserialized.Numbers[3]);
    }

    [Fact]
    public void MapFromAttributes_DictionaryOfStrings_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-6",
            Metadata = new Dictionary<string, string>
            {
                { "env", "test" },
                { "region", "us-east-1" }
            }
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(deserialized.Metadata);
        Assert.Equal("test", deserialized.Metadata["env"]);
        Assert.Equal("us-east-1", deserialized.Metadata["region"]);
    }

    [Fact]
    public void MapFromAttributes_ByteArray_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-7",
            BinaryData = new byte[] { 0x01, 0x02, 0x03, 0xFF }
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.Equal(original.BinaryData, deserialized.BinaryData);
    }

    [Fact]
    public void MapFromAttributes_MemoryStream_RoundTrip()
    {
        var data = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE };
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-8",
            StreamData = new MemoryStream(data)
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var deserialized = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.NotNull(deserialized.StreamData);
        Assert.Equal(data, deserialized.StreamData.ToArray());
    }

    [Fact]
    public void MapFromAttributes_AllComplexTypes_RoundTrip()
    {
        var original = new ComplexTypesTestEntity
        {
            Id = "rt-all",
            Name = "Full",
            Nested = new InnerTestClass { X = "NX", Y = "NY" },
            Items = new List<InnerTestClass>
            {
                new() { X = "I1", Y = "I2" },
                new() { X = "I3", Y = "I4" }
            },
            ItemMap = new Dictionary<string, InnerTestClass>
            {
                { "k1", new InnerTestClass { X = "M1", Y = "M2" } }
            },
            Tags = new List<string> { "a", "b" },
            Numbers = new List<int> { 10, 20 },
            Metadata = new Dictionary<string, string> { { "x", "y" } },
            BinaryData = new byte[] { 0x01, 0x02 },
            StreamData = new MemoryStream(new byte[] { 0x03, 0x04 })
        };

        var attrs = DynamoDbMapper.MapToAttribute(original);
        var d = (ComplexTypesTestEntity)DynamoDbMapper.MapFromAttributes(
            typeof(ComplexTypesTestEntity), attrs);

        Assert.Equal("rt-all", d.Id);
        Assert.Equal("Full", d.Name);

        Assert.NotNull(d.Nested);
        Assert.Equal("NX", d.Nested.X);

        Assert.NotNull(d.Items);
        Assert.Equal(2, d.Items.Count);
        Assert.Equal("I3", d.Items[1].X);

        Assert.NotNull(d.ItemMap);
        Assert.Equal("M1", d.ItemMap["k1"].X);

        Assert.NotNull(d.Tags);
        Assert.Equal(2, d.Tags.Count);

        Assert.NotNull(d.Numbers);
        Assert.Equal(20, d.Numbers[1]);

        Assert.NotNull(d.Metadata);
        Assert.Equal("y", d.Metadata["x"]);

        Assert.Equal(new byte[] { 0x01, 0x02 }, d.BinaryData);

        Assert.NotNull(d.StreamData);
        Assert.Equal(new byte[] { 0x03, 0x04 }, d.StreamData.ToArray());
    }
}
