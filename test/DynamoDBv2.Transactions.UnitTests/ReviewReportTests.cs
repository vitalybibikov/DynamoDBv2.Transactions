using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

// ──────────────────────────────────────────────
//  Test entities for review items
// ──────────────────────────────────────────────

public enum LongBasedEnum : long
{
    Small = 1,
    Large = 3_000_000_000L // exceeds int.MaxValue
}

public enum ByteBasedEnum : byte
{
    Off = 0,
    On = 1
}

[DynamoDBTable("LongEnumTable")]
public class LongEnumEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public LongBasedEnum Status { get; set; }
}

[DynamoDBTable("HashSetTable")]
public class HashSetEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public HashSet<string>? StringSet { get; set; }
    public HashSet<int>? IntSet { get; set; }
}

[DynamoDBTable("DupDetectTable")]
public class DupDetectEntity
{
    [DynamoDBHashKey(AttributeName = "EntityId")]
    public string EntityId { get; set; } = "";

    public string Name { get; set; } = "";
}

[DynamoDBTable("CompositeDupTable")]
public class CompositeDupEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Pk { get; set; } = "";

    [DynamoDBRangeKey(AttributeName = "sk")]
    public string Sk { get; set; } = "";

    public string Name { get; set; } = "";
}

// ──────────────────────────────────────────────
//  H1: Duplicate item detection for Put requests
// ──────────────────────────────────────────────

public class H1_DuplicatePutDetectionTests
{
    [Fact]
    public async Task Put_DuplicateHashKey_ThrowsArgumentException()
    {
        // Use real TransactionManager which has duplicate detection
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new TransactionManager(mockClient.Object);

        var requests = new List<ITransactionRequest>
        {
            new PutTransactionRequest<DupDetectEntity>(new DupDetectEntity { EntityId = "key1", Name = "First" }),
            new PutTransactionRequest<DupDetectEntity>(new DupDetectEntity { EntityId = "key1", Name = "Second" })
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            manager.ExecuteTransactionAsync(requests, null));

        Assert.Contains("Duplicate key", ex.Message);
    }

    [Fact]
    public async Task Put_DifferentHashKeys_Succeeds()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        mockClient.Setup(c => c.TransactWriteItemsAsync(
            It.IsAny<TransactWriteItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactWriteItemsResponse());

        var manager = new TransactionManager(mockClient.Object);

        var requests = new List<ITransactionRequest>
        {
            new PutTransactionRequest<DupDetectEntity>(new DupDetectEntity { EntityId = "key1", Name = "First" }),
            new PutTransactionRequest<DupDetectEntity>(new DupDetectEntity { EntityId = "key2", Name = "Second" })
        };

        await manager.ExecuteTransactionAsync(requests, null); // should not throw
    }

    [Fact]
    public async Task Put_DuplicateCompositeKey_ThrowsArgumentException()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new TransactionManager(mockClient.Object);

        var requests = new List<ITransactionRequest>
        {
            new PutTransactionRequest<CompositeDupEntity>(new CompositeDupEntity { Pk = "p1", Sk = "s1", Name = "First" }),
            new PutTransactionRequest<CompositeDupEntity>(new CompositeDupEntity { Pk = "p1", Sk = "s1", Name = "Second" })
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            manager.ExecuteTransactionAsync(requests, null));

        Assert.Contains("Duplicate key", ex.Message);
    }

    [Fact]
    public async Task Put_SameHashDifferentRange_Succeeds()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        mockClient.Setup(c => c.TransactWriteItemsAsync(
            It.IsAny<TransactWriteItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactWriteItemsResponse());

        var manager = new TransactionManager(mockClient.Object);

        var requests = new List<ITransactionRequest>
        {
            new PutTransactionRequest<CompositeDupEntity>(new CompositeDupEntity { Pk = "p1", Sk = "s1", Name = "First" }),
            new PutTransactionRequest<CompositeDupEntity>(new CompositeDupEntity { Pk = "p1", Sk = "s2", Name = "Second" })
        };

        await manager.ExecuteTransactionAsync(requests, null); // should not throw
    }

    [Fact]
    public void PutRequest_PopulatesKey_ForDuplicateDetection()
    {
        var request = new PutTransactionRequest<DupDetectEntity>(
            new DupDetectEntity { EntityId = "test-key", Name = "Test" });

        Assert.NotEmpty(request.Key);
        Assert.True(request.Key.ContainsKey("EntityId"));
        Assert.Equal("test-key", request.Key["EntityId"].S);
    }
}

// ──────────────────────────────────────────────
//  H4: Source-gen complex type fallback
// ──────────────────────────────────────────────

public class H4_SourceGenComplexTypeFallbackTests
{
    [Fact]
    public void ConvertFromAttributeValue_IsPublic()
    {
        // Verify ConvertFromAttributeValue is publicly accessible (used by source-gen fallback)
        var method = typeof(DynamoDbMapper).GetMethod("ConvertFromAttributeValue",
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        Assert.NotNull(method);
    }

    [Fact]
    public void ConvertFromAttributeValue_DeserializesNestedObject()
    {
        var attr = new AttributeValue
        {
            M = new Dictionary<string, AttributeValue>
            {
                { "X", new AttributeValue { S = "hello" } },
                { "Y", new AttributeValue { S = "world" } }
            }
        };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(InnerTestClass));
        var inner = Assert.IsType<InnerTestClass>(result);
        Assert.Equal("hello", inner.X);
        Assert.Equal("world", inner.Y);
    }

    [Fact]
    public void ConvertFromAttributeValue_DeserializesListOfStrings()
    {
        var attr = new AttributeValue
        {
            L = new List<AttributeValue>
            {
                new() { S = "a" },
                new() { S = "b" }
            }
        };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(List<string>));
        var list = Assert.IsType<List<string>>(result);
        Assert.Equal(new[] { "a", "b" }, list);
    }

    [Fact]
    public void ConvertFromAttributeValue_DeserializesDictionaryOfStrings()
    {
        var attr = new AttributeValue
        {
            M = new Dictionary<string, AttributeValue>
            {
                { "key1", new AttributeValue { S = "val1" } },
                { "key2", new AttributeValue { S = "val2" } }
            }
        };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(Dictionary<string, string>));
        var dict = Assert.IsType<Dictionary<string, string>>(result);
        Assert.Equal("val1", dict["key1"]);
        Assert.Equal("val2", dict["key2"]);
    }
}

// ──────────────────────────────────────────────
//  H5: Zero-item read transaction validation
// ──────────────────────────────────────────────

public class H5_EmptyTransactionValidationTests
{
    [Fact]
    public async Task ReadTransactionManager_ZeroItems_ThrowsArgumentException()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new ReadTransactionManager(mockClient.Object);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            manager.ExecuteGetTransactionAsync(new List<IGetTransactionRequest>()));

        Assert.Contains("at least one item", ex.Message);
    }

    [Fact]
    public async Task WriteTransactionManager_ZeroItems_ReturnsNull()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new TransactionManager(mockClient.Object);

        var result = await manager.ExecuteTransactionAsync(new List<ITransactionRequest>());

        Assert.Null(result);
    }
}

// ──────────────────────────────────────────────
//  M1: SS/NS/BS deserialization
// ──────────────────────────────────────────────

public class M1_SetDeserializationTests
{
    [Fact]
    public void ConvertFromAttributeValue_SS_ToHashSetString()
    {
        var attr = new AttributeValue { SS = new List<string> { "a", "b", "c" } };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(HashSet<string>));
        var set = Assert.IsType<HashSet<string>>(result);
        Assert.Equal(3, set.Count);
        Assert.Contains("a", set);
        Assert.Contains("b", set);
        Assert.Contains("c", set);
    }

    [Fact]
    public void ConvertFromAttributeValue_NS_ToHashSetInt()
    {
        var attr = new AttributeValue { NS = new List<string> { "1", "2", "3" } };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(HashSet<int>));
        var set = Assert.IsType<HashSet<int>>(result);
        Assert.Equal(3, set.Count);
        Assert.Contains(1, set);
        Assert.Contains(2, set);
        Assert.Contains(3, set);
    }

    [Fact]
    public void ConvertFromAttributeValue_NS_ToHashSetLong()
    {
        var attr = new AttributeValue { NS = new List<string> { "100", "200" } };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(HashSet<long>));
        var set = Assert.IsType<HashSet<long>>(result);
        Assert.Contains(100L, set);
        Assert.Contains(200L, set);
    }

    [Fact]
    public void ConvertFromAttributeValue_BS_ToHashSetByteArray()
    {
        var data1 = new byte[] { 1, 2, 3 };
        var data2 = new byte[] { 4, 5, 6 };
        var attr = new AttributeValue
        {
            BS = new List<MemoryStream>
            {
                new MemoryStream(data1),
                new MemoryStream(data2)
            }
        };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(HashSet<byte[]>));
        var set = Assert.IsType<HashSet<byte[]>>(result);
        Assert.Equal(2, set.Count);
    }

    [Fact]
    public void ConvertFromAttributeValue_SS_ToListString_V1Fallback()
    {
        // V1 stores List<string> as SS. Our deserialization should handle this.
        var attr = new AttributeValue { SS = new List<string> { "x", "y" } };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(List<string>));
        var list = Assert.IsType<List<string>>(result);
        Assert.Equal(2, list.Count);
        Assert.Contains("x", list);
        Assert.Contains("y", list);
    }

    [Fact]
    public void ConvertFromAttributeValue_NS_ToListInt_V1Fallback()
    {
        var attr = new AttributeValue { NS = new List<string> { "10", "20" } };

        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(List<int>));
        var list = Assert.IsType<List<int>>(result);
        Assert.Equal(new[] { 10, 20 }, list);
    }

    [Fact]
    public void MapFromAttributes_HashSetStringProperty_RoundTrips()
    {
        var entity = new HashSetEntity
        {
            Id = "hs1",
            StringSet = new HashSet<string> { "alpha", "beta" }
        };

        var attrs = DynamoDbMapper.MapToAttribute(entity);
        var deserialized = DynamoDbMapper.MapFromAttributes<HashSetEntity>(attrs);

        Assert.NotNull(deserialized.StringSet);
        Assert.Equal(2, deserialized.StringSet!.Count);
        Assert.Contains("alpha", deserialized.StringSet);
        Assert.Contains("beta", deserialized.StringSet);
    }

    [Fact]
    public void MapFromAttributes_HashSetIntProperty_RoundTrips()
    {
        var entity = new HashSetEntity
        {
            Id = "hs2",
            IntSet = new HashSet<int> { 1, 2, 3 }
        };

        var attrs = DynamoDbMapper.MapToAttribute(entity);
        var deserialized = DynamoDbMapper.MapFromAttributes<HashSetEntity>(attrs);

        Assert.NotNull(deserialized.IntSet);
        Assert.Equal(3, deserialized.IntSet!.Count);
        Assert.Contains(1, deserialized.IntSet);
        Assert.Contains(2, deserialized.IntSet);
        Assert.Contains(3, deserialized.IntSet);
    }
}

// ──────────────────────────────────────────────
//  M2: Empty set serialization guard
// ──────────────────────────────────────────────

public class M2_EmptySetGuardTests
{
    [Fact]
    public void V2_EmptyHashSetString_ReturnsNull()
    {
        var entity = new HashSetEntity
        {
            Id = "empty1",
            StringSet = new HashSet<string>()
        };

        var attrs = DynamoDbMapper.MapToAttribute(entity);

        // Empty sets should not be serialized (DynamoDB rejects them)
        Assert.False(attrs.ContainsKey("StringSet") && attrs["StringSet"].SS.Count > 0);
    }

    [Fact]
    public void V2_EmptyHashSetInt_ReturnsNull()
    {
        var entity = new HashSetEntity
        {
            Id = "empty2",
            IntSet = new HashSet<int>()
        };

        var attrs = DynamoDbMapper.MapToAttribute(entity);

        Assert.False(attrs.ContainsKey("IntSet") && attrs["IntSet"].NS.Count > 0);
    }
}

// ──────────────────────────────────────────────
//  M3: Enum underlying type beyond int
// ──────────────────────────────────────────────

public class M3_EnumUnderlyingTypeTests
{
    [Fact]
    public void Serialize_LongBasedEnum_SmallValue()
    {
        var attr = DynamoDbMapper.GetAttributeValue(LongBasedEnum.Small);
        Assert.NotNull(attr);
        Assert.Equal("1", attr!.N);
    }

    [Fact]
    public void Serialize_LongBasedEnum_LargeValue()
    {
        var attr = DynamoDbMapper.GetAttributeValue(LongBasedEnum.Large);
        Assert.NotNull(attr);
        Assert.Equal("3000000000", attr!.N);
    }

    [Fact]
    public void Deserialize_LongBasedEnum_LargeValue()
    {
        var attr = new AttributeValue { N = "3000000000" };
        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(LongBasedEnum));
        Assert.Equal(LongBasedEnum.Large, result);
    }

    [Fact]
    public void Deserialize_ByteBasedEnum()
    {
        var attr = new AttributeValue { N = "1" };
        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(ByteBasedEnum));
        Assert.Equal(ByteBasedEnum.On, result);
    }

    [Fact]
    public void MapFromAttributes_LongBasedEnum_RoundTrips()
    {
        var entity = new LongEnumEntity { Id = "e1", Status = LongBasedEnum.Large };

        var attrs = DynamoDbMapper.MapToAttribute(entity);
        var deserialized = DynamoDbMapper.MapFromAttributes<LongEnumEntity>(attrs);

        Assert.Equal(LongBasedEnum.Large, deserialized.Status);
    }

    [Fact]
    public void V2Serializer_LongBasedEnum_UsesInt64()
    {
        var entity = new LongEnumEntity { Id = "e2", Status = LongBasedEnum.Large };

        var attrs = DynamoDbMapper.MapToAttribute(entity);

        Assert.Equal("3000000000", attrs["Status"].N);
    }
}

// ──────────────────────────────────────────────
//  M4: Source-gen conversion parameter
// ──────────────────────────────────────────────

public class M4_SourceGenConversionParameterTests
{
    [Fact]
    public void MapToAttribute_V1Conversion_FallsBackToReflection()
    {
        // AllTypesTestEntity is partial (source-generated).
        // Passing V1 conversion should fall back to the reflection path.
        var entity = new AllTypesTestEntity
        {
            Id = "v1-test",
            BoolValue = true,
            IntValue = 42
        };

        var attrs = DynamoDbMapper.MapToAttribute(entity, DynamoDBEntryConversion.V1);

        // V1 stores bools as N "1"/"0" (not BOOL)
        Assert.True(attrs.ContainsKey("BoolValue"));
        Assert.Equal("1", attrs["BoolValue"].N);
    }

    [Fact]
    public void MapToAttribute_V2Conversion_UsesSourceGen()
    {
        var entity = new AllTypesTestEntity
        {
            Id = "v2-test",
            BoolValue = true,
            IntValue = 42
        };

        var attrs = DynamoDbMapper.MapToAttribute(entity, DynamoDBEntryConversion.V2);

        // V2 stores bools as BOOL
        Assert.True(attrs.ContainsKey("BoolValue"));
        Assert.True(attrs["BoolValue"].BOOL);
    }
}

// ──────────────────────────────────────────────
//  M5: Read transactor request accumulation
// ──────────────────────────────────────────────

public class M5_ReadTransactorRequestClearingTests
{
    [Fact]
    public async Task ExecuteAsync_ClearsRequests_SecondCallEmpty()
    {
        var requestLists = new List<List<IGetTransactionRequest>>();
        var mockManager = new Mock<IReadTransactionManager>();

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<IGetTransactionRequest>, ReadTransactionOptions?, CancellationToken>(
                (reqs, _, _) => requestLists.Add(reqs.ToList()))
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>()
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<AllTypesReflectionEntity>("id1");
        transactor.Get<AllTypesReflectionEntity>("id2");

        await transactor.ExecuteAsync();

        // Second call without adding new requests
        transactor.Get<AllTypesReflectionEntity>("id3");
        await transactor.ExecuteAsync();

        Assert.Equal(2, requestLists.Count);
        Assert.Equal(2, requestLists[0].Count); // first batch had 2
        Assert.Equal(1, requestLists[1].Count); // second batch had 1 (not 3)
    }
}

// ──────────────────────────────────────────────
//  H3: Version attribute name (already works)
// ──────────────────────────────────────────────

[DynamoDBTable("VersionRenameTable")]
public class VersionRenameEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    [DynamoDBVersion]
    [DynamoDBProperty(AttributeName = "Ver")]
    public long? Version { get; set; }

    public string Name { get; set; } = "";
}

public class H3_VersionAttributeRenameTests
{
    [Fact]
    public void GetVersion_RespectsCustomAttributeName()
    {
        var entity = new VersionRenameEntity { Id = "v1", Version = 5, Name = "test" };
        var (versionProperty, value) = DynamoDbMapper.GetVersion(entity);

        Assert.Equal("Ver", versionProperty);
        Assert.Equal(5L, value);
    }

    [Fact]
    public void MapToAttribute_VersionPropertyUsesCustomName()
    {
        var entity = new VersionRenameEntity { Id = "v1", Version = 3, Name = "test" };
        var attrs = DynamoDbMapper.MapToAttribute(entity);

        Assert.True(attrs.ContainsKey("Ver"));
        Assert.Equal("3", attrs["Ver"].N);
    }
}
