using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests that validate our library's serialization matches the AWS SDK's DynamoDBContext.
/// Pattern: Write via one system → Read via the other → Assert identical results.
/// If any test fails, it means our library serializes data differently from the SDK — that's a correctness bug.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonSerializationTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonSerializationTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task LibWrite_SdkRead_AllProperties_Match()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [1, 2, 3, 4, 5, 6, 7, 8];
        var memoryStream = new MemoryStream(bytes);

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 123456789,
            SomeNullableInt32 = 42,
            SomeLong = int.MaxValue,
            SomeNullableLong = 999,
            SomeFloat = 123.456f,
            SomeNullableFloat = 78.9f,
            SomeDecimal = 123456789.123m,
            SomeNullableDecimal = 456.789m,
            SomeDate = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc),
            SomeNullableDate1 = new DateTime(2025, 6, 14, 10, 30, 0, DateTimeKind.Utc),
            SomeBool = true,
            SomeNullableBool = false,
            SomeClass = new SomeClass { X = "TestX", Y = "TestY" },
            SomeRecord = new SomeRecord { X = "RecordX", Y = "RecordY" },
            SomeClassList = new List<SomeClass> { new() { X = "ListX", Y = "ListY" } },
            SomeBytes = bytes,
            SomeMemoryStream = memoryStream,
            SomeClassDictionary = new Dictionary<string, SomeClass> { { "Key1", new SomeClass { X = "DictX", Y = "DictY" } } }
        };

        // Write via lib
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        // Read via SDK
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        Assert.NotNull(sdkResult);
        Assert.Equal(item.UserId, sdkResult.UserId);
        Assert.Equal(item.SomeInt, sdkResult.SomeInt);
        Assert.Equal(item.SomeNullableInt32, sdkResult.SomeNullableInt32);
        Assert.Equal(item.SomeLong, sdkResult.SomeLong);
        Assert.Equal(item.SomeNullableLong, sdkResult.SomeNullableLong);
        Assert.Equal(item.SomeFloat, sdkResult.SomeFloat);
        Assert.Equal(item.SomeNullableFloat, sdkResult.SomeNullableFloat);
        Assert.Equal(item.SomeDecimal, sdkResult.SomeDecimal);
        Assert.Equal(item.SomeNullableDecimal, sdkResult.SomeNullableDecimal);
        Assert.Equal(item.SomeDate, sdkResult.SomeDate, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
        Assert.Equal(item.SomeNullableDate1!.Value, sdkResult.SomeNullableDate1!.Value, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
        Assert.Equal(item.SomeBool, sdkResult.SomeBool);
        Assert.Equal(item.SomeNullableBool, sdkResult.SomeNullableBool);
        Assert.Equal(item.SomeClass.X, sdkResult.SomeClass.X);
        Assert.Equal(item.SomeClass.Y, sdkResult.SomeClass.Y);
        Assert.Equal(item.SomeRecord.X, sdkResult.SomeRecord.X);
        Assert.Equal(item.SomeRecord.Y, sdkResult.SomeRecord.Y);
        Assert.Equal(item.SomeClassList[0].X, sdkResult.SomeClassList[0].X);
        Assert.Equal(item.SomeClassList[0].Y, sdkResult.SomeClassList[0].Y);
        Assert.Equal(item.SomeClassDictionary["Key1"].X, sdkResult.SomeClassDictionary["Key1"].X);
        Assert.Equal(item.SomeClassDictionary["Key1"].Y, sdkResult.SomeClassDictionary["Key1"].Y);
        MemoryStreamsEquality.StreamEqual(item.SomeMemoryStream, sdkResult.SomeMemoryStream);
        Assert.Equal(item.SomeBytes, sdkResult.SomeBytes);
    }

    [Fact]
    public async Task SdkWrite_LibRead_AllProperties_Match()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [10, 20, 30, 40, 50];
        var memoryStream = new MemoryStream(bytes);

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 987654321,
            SomeNullableInt32 = 77,
            SomeLong = int.MaxValue - 1,
            SomeNullableLong = 888,
            SomeFloat = 456.789f,
            SomeNullableFloat = 12.3f,
            SomeDecimal = 987654321.987m,
            SomeNullableDecimal = 321.654m,
            SomeDate = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            SomeNullableDate1 = new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc),
            SomeBool = false,
            SomeNullableBool = true,
            SomeClass = new SomeClass { X = "SdkX", Y = "SdkY" },
            SomeRecord = new SomeRecord { X = "SdkRecX", Y = "SdkRecY" },
            SomeClassList = new List<SomeClass> { new() { X = "SdkListX", Y = "SdkListY" } },
            SomeBytes = bytes,
            SomeMemoryStream = memoryStream,
            SomeClassDictionary = new Dictionary<string, SomeClass> { { "SdkKey", new SomeClass { X = "SdkDictX", Y = "SdkDictY" } } }
        };

        // Write via SDK
        await _fixture.Db.Context.SaveAsync(item);

        // Read via lib (TransactGetItems)
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(item.UserId, libResult.UserId);
        Assert.Equal(item.SomeInt, libResult.SomeInt);
        Assert.Equal(item.SomeNullableInt32, libResult.SomeNullableInt32);
        Assert.Equal(item.SomeLong, libResult.SomeLong);
        Assert.Equal(item.SomeNullableLong, libResult.SomeNullableLong);
        Assert.Equal(item.SomeFloat, libResult.SomeFloat);
        Assert.Equal(item.SomeNullableFloat, libResult.SomeNullableFloat);
        Assert.Equal(item.SomeDecimal, libResult.SomeDecimal);
        Assert.Equal(item.SomeNullableDecimal, libResult.SomeNullableDecimal);
        Assert.Equal(item.SomeDate, libResult.SomeDate, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
        Assert.Equal(item.SomeNullableDate1!.Value, libResult.SomeNullableDate1!.Value, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
        Assert.Equal(item.SomeBool, libResult.SomeBool);
        Assert.Equal(item.SomeNullableBool, libResult.SomeNullableBool);
        Assert.Equal(item.SomeClass.X, libResult.SomeClass!.X);
        Assert.Equal(item.SomeClass.Y, libResult.SomeClass.Y);
        Assert.Equal(item.SomeRecord.X, libResult.SomeRecord!.X);
        Assert.Equal(item.SomeRecord.Y, libResult.SomeRecord.Y);
        Assert.Equal(item.SomeClassList[0].X, libResult.SomeClassList![0].X);
        Assert.Equal(item.SomeClassList[0].Y, libResult.SomeClassList[0].Y);
        Assert.Equal(item.SomeClassDictionary["SdkKey"].X, libResult.SomeClassDictionary!["SdkKey"].X);
        Assert.Equal(item.SomeClassDictionary["SdkKey"].Y, libResult.SomeClassDictionary["SdkKey"].Y);
        MemoryStreamsEquality.StreamEqual(item.SomeMemoryStream, libResult.SomeMemoryStream);
        Assert.Equal(item.SomeBytes, libResult.SomeBytes);
    }

    [Fact]
    public async Task LibWrite_SdkRead_AllNullableFieldsNull()
    {
        var userId = Guid.NewGuid().ToString();

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 2,
            SomeFloat = 1.0f,
            SomeDecimal = 1.0m,
            SomeDate = DateTime.UtcNow,
            SomeBool = false,
            SomeNullableInt32 = null,
            SomeNullableLong = null,
            SomeNullableFloat = null,
            SomeNullableDecimal = null,
            SomeNullableDate1 = null,
            SomeNullableBool = null,
            SomeClass = null,
            SomeRecord = null,
            SomeClassList = null,
            SomeClassDictionary = null,
            SomeMemoryStream = null,
            SomeBytes = null
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        Assert.NotNull(sdkResult);
        Assert.Null(sdkResult.SomeNullableInt32);
        Assert.Null(sdkResult.SomeNullableLong);
        Assert.Null(sdkResult.SomeNullableFloat);
        Assert.Null(sdkResult.SomeNullableDecimal);
        Assert.Null(sdkResult.SomeNullableDate1);
        Assert.Null(sdkResult.SomeNullableBool);
        Assert.Null(sdkResult.SomeClass);
        Assert.Null(sdkResult.SomeRecord);
        Assert.Null(sdkResult.SomeClassList);
        Assert.Null(sdkResult.SomeClassDictionary);
        Assert.Null(sdkResult.SomeMemoryStream);
        Assert.Null(sdkResult.SomeBytes);
    }

    [Fact]
    public async Task SdkWrite_LibRead_AllNullableFieldsNull()
    {
        var userId = Guid.NewGuid().ToString();

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 2,
            SomeFloat = 1.0f,
            SomeDecimal = 1.0m,
            SomeDate = DateTime.UtcNow,
            SomeBool = false,
            SomeNullableInt32 = null,
            SomeNullableLong = null,
            SomeNullableFloat = null,
            SomeNullableDecimal = null,
            SomeNullableDate1 = null,
            SomeNullableBool = null,
            SomeClass = null,
            SomeRecord = null,
            SomeClassList = null,
            SomeClassDictionary = null,
            SomeMemoryStream = null,
            SomeBytes = null
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Null(libResult.SomeNullableInt32);
        Assert.Null(libResult.SomeNullableLong);
        Assert.Null(libResult.SomeNullableFloat);
        Assert.Null(libResult.SomeNullableDecimal);
        Assert.Null(libResult.SomeNullableDate1);
        Assert.Null(libResult.SomeNullableBool);
        Assert.Null(libResult.SomeClass);
        Assert.Null(libResult.SomeRecord);
        Assert.Null(libResult.SomeClassList);
        Assert.Null(libResult.SomeClassDictionary);
        Assert.Null(libResult.SomeMemoryStream);
        Assert.Null(libResult.SomeBytes);
    }

    [Fact]
    public async Task LibWrite_SdkRead_DefaultValues_Match()
    {
        var userId = Guid.NewGuid().ToString();

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = default,
            SomeLong = default,
            SomeFloat = default,
            SomeDecimal = default,
            SomeDate = default(DateTime).ToUniversalTime(),
            SomeBool = default
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        Assert.NotNull(sdkResult);
        Assert.Equal(0, sdkResult.SomeInt);
        Assert.Equal(0, sdkResult.SomeLong);
        Assert.Equal(0f, sdkResult.SomeFloat);
        Assert.Equal(0m, sdkResult.SomeDecimal);
        Assert.False(sdkResult.SomeBool);
    }

    [Fact]
    public async Task SdkWrite_LibRead_DefaultValues_Match()
    {
        var userId = Guid.NewGuid().ToString();

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = default,
            SomeLong = default,
            SomeFloat = default,
            SomeDecimal = default,
            SomeDate = default(DateTime).ToUniversalTime(),
            SomeBool = default
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(0, libResult.SomeInt);
        Assert.Equal(0, libResult.SomeLong);
        Assert.Equal(0f, libResult.SomeFloat);
        Assert.Equal(0m, libResult.SomeDecimal);
        Assert.False(libResult.SomeBool);
    }

    [Fact]
    public async Task LibWrite_SdkRead_MaxValues_Match()
    {
        var userId = Guid.NewGuid().ToString();

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = int.MaxValue,
            SomeLong = int.MaxValue,
            SomeFloat = float.MaxValue,
            SomeDecimal = decimal.MaxValue,
            SomeDate = DateTime.MaxValue,
            SomeBool = true,
            SomeNullableInt32 = int.MaxValue,
            SomeNullableLong = int.MaxValue,
            SomeNullableFloat = float.MaxValue,
            SomeNullableDecimal = decimal.MaxValue,
            SomeNullableBool = true
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        Assert.NotNull(sdkResult);
        Assert.Equal(int.MaxValue, sdkResult.SomeInt);
        Assert.Equal(int.MaxValue, sdkResult.SomeLong);
        Assert.Equal(float.MaxValue, sdkResult.SomeFloat);
        Assert.Equal(decimal.MaxValue, sdkResult.SomeDecimal);
        Assert.True(sdkResult.SomeBool);
        Assert.Equal(int.MaxValue, sdkResult.SomeNullableInt32);
        Assert.Equal(int.MaxValue, sdkResult.SomeNullableLong);
        Assert.Equal(float.MaxValue, sdkResult.SomeNullableFloat);
        Assert.Equal(decimal.MaxValue, sdkResult.SomeNullableDecimal);
        Assert.True(sdkResult.SomeNullableBool);
    }

    [Fact]
    public async Task SdkWrite_LibRead_MaxValues_Match()
    {
        var userId = Guid.NewGuid().ToString();

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = int.MaxValue,
            SomeLong = int.MaxValue,
            SomeFloat = float.MaxValue,
            SomeDecimal = decimal.MaxValue,
            SomeDate = DateTime.MaxValue,
            SomeBool = true,
            SomeNullableInt32 = int.MaxValue,
            SomeNullableLong = int.MaxValue,
            SomeNullableFloat = float.MaxValue,
            SomeNullableDecimal = decimal.MaxValue,
            SomeNullableBool = true
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(int.MaxValue, libResult.SomeInt);
        Assert.Equal(int.MaxValue, libResult.SomeLong);
        Assert.Equal(float.MaxValue, libResult.SomeFloat);
        Assert.Equal(decimal.MaxValue, libResult.SomeDecimal);
        Assert.True(libResult.SomeBool);
        Assert.Equal(int.MaxValue, libResult.SomeNullableInt32);
        Assert.Equal(int.MaxValue, libResult.SomeNullableLong);
        Assert.Equal(float.MaxValue, libResult.SomeNullableFloat);
        Assert.Equal(decimal.MaxValue, libResult.SomeNullableDecimal);
        Assert.True(libResult.SomeNullableBool);
    }

    [Fact]
    public async Task LibWrite_SdkRead_NestedClass_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeClass = new SomeClass { X = "NestedX", Y = "NestedY" }
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult?.SomeClass);
        Assert.Equal("NestedX", sdkResult.SomeClass.X);
        Assert.Equal("NestedY", sdkResult.SomeClass.Y);
    }

    [Fact]
    public async Task SdkWrite_LibRead_NestedClass_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeClass = new SomeClass { X = "SdkNestedX", Y = "SdkNestedY" }
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult?.SomeClass);
        Assert.Equal("SdkNestedX", libResult.SomeClass.X);
        Assert.Equal("SdkNestedY", libResult.SomeClass.Y);
    }

    [Fact]
    public async Task LibWrite_SdkRead_Record_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeRecord = new SomeRecord { X = "RecX", Y = "RecY" }
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult?.SomeRecord);
        Assert.Equal("RecX", sdkResult.SomeRecord.X);
        Assert.Equal("RecY", sdkResult.SomeRecord.Y);
    }

    [Fact]
    public async Task SdkWrite_LibRead_Record_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeRecord = new SomeRecord { X = "SdkRecX", Y = "SdkRecY" }
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult?.SomeRecord);
        Assert.Equal("SdkRecX", libResult.SomeRecord.X);
        Assert.Equal("SdkRecY", libResult.SomeRecord.Y);
    }

    [Fact]
    public async Task LibWrite_SdkRead_ListOfClasses_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeClassList = new List<SomeClass>
            {
                new() { X = "L1X", Y = "L1Y" },
                new() { X = "L2X", Y = "L2Y" },
                new() { X = "L3X", Y = "L3Y" }
            }
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult?.SomeClassList);
        Assert.Equal(3, sdkResult.SomeClassList.Count);
        Assert.Equal("L1X", sdkResult.SomeClassList[0].X);
        Assert.Equal("L2X", sdkResult.SomeClassList[1].X);
        Assert.Equal("L3X", sdkResult.SomeClassList[2].X);
    }

    [Fact]
    public async Task SdkWrite_LibRead_ListOfClasses_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeClassList = new List<SomeClass>
            {
                new() { X = "SL1X", Y = "SL1Y" },
                new() { X = "SL2X", Y = "SL2Y" }
            }
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult?.SomeClassList);
        Assert.Equal(2, libResult.SomeClassList.Count);
        Assert.Equal("SL1X", libResult.SomeClassList[0].X);
        Assert.Equal("SL2X", libResult.SomeClassList[1].X);
    }

    [Fact]
    public async Task LibWrite_SdkRead_Dictionary_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeClassDictionary = new Dictionary<string, SomeClass>
            {
                { "Alpha", new SomeClass { X = "AX", Y = "AY" } },
                { "Beta", new SomeClass { X = "BX", Y = "BY" } }
            }
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult?.SomeClassDictionary);
        Assert.Equal(2, sdkResult.SomeClassDictionary.Count);
        Assert.Equal("AX", sdkResult.SomeClassDictionary["Alpha"].X);
        Assert.Equal("BX", sdkResult.SomeClassDictionary["Beta"].X);
    }

    [Fact]
    public async Task SdkWrite_LibRead_Dictionary_Match()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeClassDictionary = new Dictionary<string, SomeClass>
            {
                { "Gamma", new SomeClass { X = "GX", Y = "GY" } }
            }
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult?.SomeClassDictionary);
        Assert.Single(libResult.SomeClassDictionary);
        Assert.Equal("GX", libResult.SomeClassDictionary["Gamma"].X);
    }

    [Fact]
    public async Task LibWrite_SdkRead_BinaryData_Match()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [0, 1, 127, 128, 254, 255];
        var memoryStream = new MemoryStream(bytes);

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeBytes = bytes,
            SomeMemoryStream = memoryStream
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult);
        Assert.Equal(bytes, sdkResult.SomeBytes);
        MemoryStreamsEquality.StreamEqual(new MemoryStream(bytes), sdkResult.SomeMemoryStream);
    }

    [Fact]
    public async Task SdkWrite_LibRead_BinaryData_Match()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [100, 200, 50, 75];
        var memoryStream = new MemoryStream(bytes);

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeBytes = bytes,
            SomeMemoryStream = memoryStream
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(bytes, libResult.SomeBytes);
        MemoryStreamsEquality.StreamEqual(new MemoryStream(bytes), libResult.SomeMemoryStream);
    }

    [Fact]
    public async Task BidirectionalRoundTrip_LibWrite_SdkRead_SdkUpdate_LibRead()
    {
        var userId = Guid.NewGuid().ToString();

        // Step 1: Write via lib
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 100,
            SomeDecimal = 50.5m,
            SomeFloat = 1.5f,
            SomeLong = 1000,
            SomeDate = new DateTime(2025, 6, 15, 10, 0, 0, DateTimeKind.Utc),
            SomeBool = true,
            SomeClass = new SomeClass { X = "Original", Y = "Value" }
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        // Step 2: Read via SDK and update
        var sdkLoaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkLoaded);
        Assert.Equal(100, sdkLoaded.SomeInt);

        sdkLoaded.SomeInt = 200;
        sdkLoaded.SomeClass = new SomeClass { X = "Updated", Y = "BySdk" };
        await _fixture.Db.Context.SaveAsync(sdkLoaded);

        // Step 3: Read via lib and verify update
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(200, libResult.SomeInt);
        Assert.Equal("Updated", libResult.SomeClass!.X);
        Assert.Equal("BySdk", libResult.SomeClass.Y);
        Assert.Equal(50.5m, libResult.SomeDecimal); // unchanged
        Assert.True(libResult.SomeBool); // unchanged
    }

    [Fact]
    public async Task LibWrite_SdkRead_MultipleItems_AllMatch()
    {
        var userId1 = Guid.NewGuid().ToString();
        var userId2 = Guid.NewGuid().ToString();
        var userId3 = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId1, SomeInt = 111, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
            writer.CreateOrUpdate(new TestTable { UserId = userId2, SomeInt = 222, SomeLong = 2, SomeFloat = 2f, SomeDecimal = 2m, SomeDate = DateTime.UtcNow });
            writer.CreateOrUpdate(new TestTable { UserId = userId3, SomeInt = 333, SomeLong = 3, SomeFloat = 3f, SomeDecimal = 3m, SomeDate = DateTime.UtcNow });
        }

        var sdk1 = await _fixture.Db.Context.LoadAsync<TestTable>(userId1);
        var sdk2 = await _fixture.Db.Context.LoadAsync<TestTable>(userId2);
        var sdk3 = await _fixture.Db.Context.LoadAsync<TestTable>(userId3);

        Assert.Equal(111, sdk1.SomeInt);
        Assert.Equal(222, sdk2.SomeInt);
        Assert.Equal(333, sdk3.SomeInt);
    }

    [Fact]
    public async Task SdkWrite_LibRead_MultipleItems_AllMatch()
    {
        var userId1 = Guid.NewGuid().ToString();
        var userId2 = Guid.NewGuid().ToString();

        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId1, SomeInt = 444, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId2, SomeInt = 555, SomeLong = 2, SomeFloat = 2f, SomeDecimal = 2m, SomeDate = DateTime.UtcNow });

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId1);
        reader.Get<TestTable>(userId2);
        var result = await reader.ExecuteAsync();

        Assert.Equal(444, result.GetItem<TestTable>(0)!.SomeInt);
        Assert.Equal(555, result.GetItem<TestTable>(1)!.SomeInt);
    }

    [Fact]
    public async Task LibWrite_BothReadPaths_ReturnIdenticalResults()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [9, 8, 7];
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 42,
            SomeNullableInt32 = 7,
            SomeLong = 100,
            SomeFloat = 3.14f,
            SomeDecimal = 99.99m,
            SomeDate = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc),
            SomeBool = true,
            SomeNullableBool = false,
            SomeClass = new SomeClass { X = "CX", Y = "CY" },
            SomeBytes = bytes
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        // Read via SDK
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        // Read via lib
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        // Both should return identical results
        Assert.NotNull(sdkResult);
        Assert.NotNull(libResult);
        Assert.Equal(sdkResult.SomeInt, libResult.SomeInt);
        Assert.Equal(sdkResult.SomeNullableInt32, libResult.SomeNullableInt32);
        Assert.Equal(sdkResult.SomeLong, libResult.SomeLong);
        Assert.Equal(sdkResult.SomeFloat, libResult.SomeFloat);
        Assert.Equal(sdkResult.SomeDecimal, libResult.SomeDecimal);
        Assert.Equal(sdkResult.SomeDate, libResult.SomeDate, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
        Assert.Equal(sdkResult.SomeBool, libResult.SomeBool);
        Assert.Equal(sdkResult.SomeNullableBool, libResult.SomeNullableBool);
        Assert.Equal(sdkResult.SomeClass?.X, libResult.SomeClass?.X);
        Assert.Equal(sdkResult.SomeClass?.Y, libResult.SomeClass?.Y);
        Assert.Equal(sdkResult.SomeBytes, libResult.SomeBytes);
        Assert.Equal(sdkResult.Version, libResult.Version);
    }
}
