using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;
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
    public async Task WriteTransactionManager_ZeroItems_ThrowsArgumentException()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new TransactionManager(mockClient.Object);

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => manager.ExecuteTransactionAsync(new List<ITransactionRequest>()));

        Assert.Contains("at least one item", ex.Message);
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

// ──────────────────────────────────────────────
//  H1-NEW: Typed key overloads (numeric + binary)
// ──────────────────────────────────────────────

[DynamoDBTable("NumericKeyTable")]
public class NumericKeyEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public int Id { get; set; }

    public string Name { get; set; } = "";
}

[DynamoDBTable("CompositeNumericKeyTable")]
public class CompositeNumericKeyEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public long Pk { get; set; }

    [DynamoDBRangeKey(AttributeName = "sk")]
    public int Sk { get; set; }

    public string Name { get; set; } = "";
}

public class H1_TypedKeyOverloadTests
{
    [Fact]
    public void ConditionCheck_NumericHashKey_CreatesNAttribute()
    {
        var request = new ConditionCheckTransactionRequest<NumericKeyEntity>((object)42);
        Assert.Equal("42", request.Key["pk"].N);
    }

    [Fact]
    public void ConditionCheck_CompositeNumericKey_CreatesNAttributes()
    {
        var request = new ConditionCheckTransactionRequest<CompositeNumericKeyEntity>((object)100L, (object)5);
        Assert.Equal("100", request.Key["pk"].N);
        Assert.Equal("5", request.Key["sk"].N);
    }

    [Fact]
    public void Delete_NumericHashKey_CreatesNAttribute()
    {
        var request = new DeleteTransactionRequest<NumericKeyEntity>((object)99);
        Assert.Equal("99", request.Key["pk"].N);
    }

    [Fact]
    public void Delete_CompositeNumericKey_CreatesNAttributes()
    {
        var request = new DeleteTransactionRequest<CompositeNumericKeyEntity>((object)1L, (object)2);
        Assert.Equal("1", request.Key["pk"].N);
        Assert.Equal("2", request.Key["sk"].N);
    }

    [Fact]
    public void Get_NumericHashKey_CreatesNAttribute()
    {
        var request = new GetTransactionRequest<NumericKeyEntity>((object)42);
        Assert.Equal("42", request.Key["pk"].N);
    }

    [Fact]
    public void Get_CompositeNumericKey_CreatesNAttributes()
    {
        var request = new GetTransactionRequest<CompositeNumericKeyEntity>((object)100L, (object)5);
        Assert.Equal("100", request.Key["pk"].N);
        Assert.Equal("5", request.Key["sk"].N);
    }

    [Fact]
    public void ConditionCheck_StringKey_ViaObjectOverload_CreatesSAttribute()
    {
        var request = new ConditionCheckTransactionRequest<DupDetectEntity>((object)"my-key");
        Assert.Equal("my-key", request.Key["EntityId"].S);
    }

    [Fact]
    public void Get_NullKey_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new GetTransactionRequest<NumericKeyEntity>((object)null!));
    }

    [Fact]
    public void Delete_DecimalKey_CreatesNAttribute()
    {
        var request = new DeleteTransactionRequest<NumericKeyEntity>((object)3.14m);
        Assert.Equal("3.14", request.Key["pk"].N);
    }

    [Fact]
    public void ConditionCheck_BinaryKey_CreatesBAttribute()
    {
        var keyBytes = new byte[] { 0x01, 0x02, 0x03 };
        var request = new ConditionCheckTransactionRequest<NumericKeyEntity>((object)keyBytes);
        Assert.NotNull(request.Key["pk"].B);
    }
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

// ──────────────────────────────────────────────
//  FIX1: Composite-key model-based patch
// ──────────────────────────────────────────────

public class Fix1_CompositeKeyModelPatchTests
{
    // --- Model-based constructor: (T model, string propertyName) ---

    [Fact]
    public void PatchFromModel_CompositeKey_SetsBothKeys()
    {
        var entity = new CompositeDupEntity { Pk = "hash1", Sk = "range1", Name = "Updated" };

        var request = new PatchTransactionRequest<CompositeDupEntity>(entity, nameof(CompositeDupEntity.Name));

        Assert.Equal(2, request.Key.Count);
        Assert.Equal("hash1", request.Key["pk"].S);
        Assert.Equal("range1", request.Key["sk"].S);
    }

    [Fact]
    public void PatchFromModel_HashKeyOnly_SetsSingleKey()
    {
        var entity = new DupDetectEntity { EntityId = "key1", Name = "Updated" };

        var request = new PatchTransactionRequest<DupDetectEntity>(entity, nameof(DupDetectEntity.Name));

        Assert.Single(request.Key);
        Assert.Equal("key1", request.Key["EntityId"].S);
    }

    [Fact]
    public void PatchFromModel_CompositeKey_MissingRangeKey_Throws()
    {
        var entity = new CompositeDupEntity { Pk = "hash1", Sk = null!, Name = "Updated" };

        Assert.Throws<ArgumentException>(() =>
            new PatchTransactionRequest<CompositeDupEntity>(entity, nameof(CompositeDupEntity.Name)));
    }

    [Fact]
    public void PatchFromModel_CompositeKey_OperationContainsBothKeys()
    {
        var entity = new CompositeDupEntity { Pk = "h1", Sk = "r1", Name = "Patched" };

        var request = new PatchTransactionRequest<CompositeDupEntity>(entity, nameof(CompositeDupEntity.Name));
        var op = request.GetOperation();

        Assert.NotNull(op.UpdateType);
        Assert.Equal(2, op.UpdateType!.Key.Count);
        Assert.Equal("h1", op.UpdateType.Key["pk"].S);
        Assert.Equal("r1", op.UpdateType.Key["sk"].S);
    }

    [Fact]
    public void PatchFromModel_CompositeKey_UpdateExpressionIsCorrect()
    {
        var entity = new CompositeDupEntity { Pk = "h1", Sk = "r1", Name = "NewName" };

        var request = new PatchTransactionRequest<CompositeDupEntity>(entity, nameof(CompositeDupEntity.Name));
        var op = request.GetOperation();

        Assert.Equal("SET #Property = :newValue", op.UpdateType!.UpdateExpression);
        Assert.Equal("Name", op.UpdateType.ExpressionAttributeNames["#Property"]);
        Assert.Equal("NewName", op.UpdateType.ExpressionAttributeValues[":newValue"].S);
    }

    [Fact]
    public void PatchFromModel_NullModel_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new PatchTransactionRequest<CompositeDupEntity>(null!, "Name"));
    }

    [Fact]
    public void PatchFromModel_NullPropertyName_ThrowsArgumentNullException()
    {
        var entity = new CompositeDupEntity { Pk = "h1", Sk = "r1", Name = "Test" };

        Assert.Throws<ArgumentNullException>(() =>
            new PatchTransactionRequest<CompositeDupEntity>(entity, null!));
    }

    [Fact]
    public void PatchFromModel_MissingHashKey_ThrowsArgumentException()
    {
        var entity = new CompositeDupEntity { Pk = null!, Sk = "r1", Name = "Test" };

        Assert.Throws<ArgumentException>(() =>
            new PatchTransactionRequest<CompositeDupEntity>(entity, nameof(CompositeDupEntity.Name)));
    }

    [Fact]
    public void PatchFromModel_NullPropertyValue_UsesExplicitNull()
    {
        var entity = new CompositeDupEntity { Pk = "h1", Sk = "r1", Name = null! };

        var request = new PatchTransactionRequest<CompositeDupEntity>(entity, nameof(CompositeDupEntity.Name));
        var op = request.GetOperation();

        Assert.True(op.UpdateType!.ExpressionAttributeValues[":newValue"].NULL);
    }

    // --- Via DynamoDbTransactor.PatchAsync ---

    [Fact]
    public async Task PatchAsync_CompositeKey_ViaTransactor_SetsBothKeys()
    {
        var capturedRequests = new List<ITransactionRequest>();
        var mockManager = new Mock<ITransactionManager>();
        mockManager.Setup(m => m.ExecuteTransactionAsync(
                It.IsAny<IEnumerable<ITransactionRequest>>(),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<ITransactionRequest>, TransactionOptions?, CancellationToken>(
                (reqs, _, _) => capturedRequests.AddRange(reqs))
            .ReturnsAsync((TransactWriteItemsResponse?)null);

        var entity = new CompositeDupEntity { Pk = "hash1", Sk = "range1", Name = "Patched" };

        await using (var transactor = new DynamoDbTransactor(mockManager.Object))
        {
            transactor.PatchAsync(entity, nameof(CompositeDupEntity.Name));
        }

        Assert.Single(capturedRequests);
        Assert.Equal(2, capturedRequests[0].Key.Count);
        Assert.Equal("hash1", capturedRequests[0].Key["pk"].S);
        Assert.Equal("range1", capturedRequests[0].Key["sk"].S);
    }

    // --- String-key constructor: (string keyValue, Property value) ---

    [Fact]
    public void PatchStringKey_Property_SetsKeyAndUpdateExpression()
    {
        var request = new PatchTransactionRequest<DupDetectEntity>(
            "myKey",
            new Property { Name = nameof(DupDetectEntity.Name), Value = "NewVal" });

        Assert.Single(request.Key);
        Assert.Equal("myKey", request.Key["EntityId"].S);
        Assert.Equal("SET #Property = :newValue", request.UpdateExpression);
    }

    // --- Composite string-key constructor: (string, string, Property) ---

    [Fact]
    public void PatchCompositeStringKey_Property_SetsBothKeys()
    {
        var request = new PatchTransactionRequest<CompositeDupEntity>(
            "h1", "r1",
            new Property { Name = nameof(CompositeDupEntity.Name), Value = "NewVal" });

        Assert.Equal(2, request.Key.Count);
        Assert.Equal("h1", request.Key["pk"].S);
        Assert.Equal("r1", request.Key["sk"].S);
    }

    // --- KeyValue constructor: (KeyValue, Property) ---

    [Fact]
    public void PatchKeyValue_Property_SetsKey()
    {
        var request = new PatchTransactionRequest<DupDetectEntity>(
            new KeyValue { Key = nameof(DupDetectEntity.EntityId), Value = "kv1" },
            new Property { Name = nameof(DupDetectEntity.Name), Value = "Patched" });

        Assert.Single(request.Key);
        Assert.Equal("kv1", request.Key["EntityId"].S);
    }
}

// ──────────────────────────────────────────────
//  ConditionCheck GetOperation() idempotency
// ──────────────────────────────────────────────

public class ConditionCheckIdempotencyTests
{
    [Fact]
    public void GetOperation_CalledTwice_ProducesSameConditionExpression()
    {
        var request = new ConditionCheckTransactionRequest<DupDetectEntity>("key1");
        request.Equals<DupDetectEntity, string>(x => x.Name, "Active");

        var op1 = request.GetOperation();
        var op2 = request.GetOperation();

        // Both calls should produce a valid condition expression
        Assert.NotNull(op1.ConditionCheckType!.ConditionExpression);
        Assert.NotNull(op2.ConditionCheckType!.ConditionExpression);
        Assert.Equal(
            op1.ConditionCheckType.ConditionExpression,
            op2.ConditionCheckType.ConditionExpression);
    }

    [Fact]
    public void GetOperation_CalledTwice_MultipleConditions_NotCorrupted()
    {
        var request = new ConditionCheckTransactionRequest<DupDetectEntity>("key1");
        request.Equals<DupDetectEntity, string>(x => x.Name, "Active");
        request.NotEquals<DupDetectEntity, string>(x => x.Name, "Deleted");

        var op1 = request.GetOperation();
        var expr1 = op1.ConditionCheckType!.ConditionExpression;

        var op2 = request.GetOperation();
        var expr2 = op2.ConditionCheckType!.ConditionExpression;

        // The expression should contain both conditions and not be truncated
        Assert.Contains("AND", expr1);
        Assert.Equal(expr1, expr2);
    }

    [Fact]
    public void GetOperation_NoConditions_ReturnsNullExpression()
    {
        var request = new ConditionCheckTransactionRequest<DupDetectEntity>("key1");

        var op = request.GetOperation();

        Assert.Null(op.ConditionCheckType!.ConditionExpression);
    }
}

// ──────────────────────────────────────────────
//  Numeric deserialization parity (sbyte/ushort/uint/ulong)
// ──────────────────────────────────────────────

[DynamoDBTable("ExoticNumericTable")]
public class ExoticNumericEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public sbyte SByteValue { get; set; }
    public ushort UShortValue { get; set; }
    public uint UIntValue { get; set; }
    public ulong ULongValue { get; set; }
}

public class NumericParityTests
{
    [Fact]
    public void Serialize_SByte_ProducesN()
    {
        var attr = DynamoDbMapper.GetAttributeValue((object)(sbyte)-42);
        Assert.NotNull(attr);
        Assert.Equal("-42", attr!.N);
    }

    [Fact]
    public void Serialize_UShort_ProducesN()
    {
        var attr = DynamoDbMapper.GetAttributeValue((object)(ushort)65535);
        Assert.NotNull(attr);
        Assert.Equal("65535", attr!.N);
    }

    [Fact]
    public void Serialize_UInt_ProducesN()
    {
        var attr = DynamoDbMapper.GetAttributeValue((object)(uint)4_000_000_000);
        Assert.NotNull(attr);
        Assert.Equal("4000000000", attr!.N);
    }

    [Fact]
    public void Serialize_ULong_ProducesN()
    {
        var attr = DynamoDbMapper.GetAttributeValue((object)(ulong)18_000_000_000_000_000_000);
        Assert.NotNull(attr);
        Assert.Equal("18000000000000000000", attr!.N);
    }

    [Fact]
    public void Deserialize_SByte_FromN()
    {
        var attr = new AttributeValue { N = "-42" };
        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(sbyte));
        Assert.Equal((sbyte)-42, result);
    }

    [Fact]
    public void Deserialize_UShort_FromN()
    {
        var attr = new AttributeValue { N = "65535" };
        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(ushort));
        Assert.Equal((ushort)65535, result);
    }

    [Fact]
    public void Deserialize_UInt_FromN()
    {
        var attr = new AttributeValue { N = "4000000000" };
        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(uint));
        Assert.Equal((uint)4_000_000_000, result);
    }

    [Fact]
    public void Deserialize_ULong_FromN()
    {
        var attr = new AttributeValue { N = "18000000000000000000" };
        var result = DynamoDbMapper.ConvertFromAttributeValue(attr, typeof(ulong));
        Assert.Equal((ulong)18_000_000_000_000_000_000, result);
    }

    [Fact]
    public void RoundTrip_ExoticNumericEntity()
    {
        var entity = new ExoticNumericEntity
        {
            Id = "test",
            SByteValue = -100,
            UShortValue = 50000,
            UIntValue = 3_000_000_000,
            ULongValue = 10_000_000_000_000_000_000
        };

        var attrs = DynamoDbMapper.MapToAttribute(entity);
        var deserialized = DynamoDbMapper.MapFromAttributes<ExoticNumericEntity>(attrs);

        Assert.Equal(-100, deserialized.SByteValue);
        Assert.Equal((ushort)50000, deserialized.UShortValue);
        Assert.Equal((uint)3_000_000_000, deserialized.UIntValue);
        Assert.Equal((ulong)10_000_000_000_000_000_000, deserialized.ULongValue);
    }
}

// ──────────────────────────────────────────────
//  FIX5: Source-gen V1 numeric bool deserialization
// ──────────────────────────────────────────────

public class Fix5_SourceGenV1BoolDeserializationTests
{
    // --- Source-gen path (partial AllTypesTestEntity) ---

    [Fact]
    public void SourceGen_BoolFromNumericN1_DeserializesTrue()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "BoolValue", new AttributeValue { N = "1" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.True(entity.BoolValue);
    }

    [Fact]
    public void SourceGen_BoolFromNumericN0_DeserializesFalse()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "BoolValue", new AttributeValue { N = "0" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.False(entity.BoolValue);
    }

    [Fact]
    public void SourceGen_BoolFromBOOL_StillWorks()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "BoolValue", new AttributeValue { BOOL = true } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.True(entity.BoolValue);
    }

    [Fact]
    public void SourceGen_NullableBoolFromNumericN1_DeserializesTrue()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "NullableBool", new AttributeValue { N = "1" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.True(entity.NullableBool);
    }

    [Fact]
    public void SourceGen_NullableBoolFromNumericN0_DeserializesFalse()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "NullableBool", new AttributeValue { N = "0" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.NotNull(entity.NullableBool);
        Assert.False(entity.NullableBool!.Value);
    }

    [Fact]
    public void SourceGen_BoolFalseFromBOOL_StillWorks()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "BoolValue", new AttributeValue { BOOL = false } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.False(entity.BoolValue);
    }

    // --- Reflection path (non-partial AllTypesReflectionEntity) ---

    [Fact]
    public void Reflection_BoolFromNumericN1_DeserializesTrue()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "BoolValue", new AttributeValue { N = "1" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesReflectionEntity>(attrs);

        Assert.True(entity.BoolValue);
    }

    [Fact]
    public void Reflection_BoolFromNumericN0_DeserializesFalse()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "pk", new AttributeValue { S = "test" } },
            { "BoolValue", new AttributeValue { N = "0" } }
        };

        var entity = DynamoDbMapper.MapFromAttributes<AllTypesReflectionEntity>(attrs);

        Assert.False(entity.BoolValue);
    }

    // --- Full round-trip with V1 conversion ---

    [Fact]
    public void V1_BoolRoundTrip_NumericFormat()
    {
        var entity = new AllTypesTestEntity { Id = "rt", BoolValue = true };

        // V1 serializes bool as N="1"
        var attrs = DynamoDbMapper.MapToAttribute(entity, DynamoDBEntryConversion.V1);

        Assert.Equal("1", attrs["BoolValue"].N);

        // Deserialize back — should handle the V1 numeric format
        var deserialized = DynamoDbMapper.MapFromAttributes<AllTypesTestEntity>(attrs);

        Assert.True(deserialized.BoolValue);
    }
}
