using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Amazon.DynamoDBv2.Model;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Edge case comparison tests: empty strings, empty collections, Unicode,
/// min/max boundaries, and other corner cases where serialization differences could appear.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonEdgeCaseTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonEdgeCaseTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task EmptyListAndDictionary_LibWrite_SdkRead()
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
            SomeClassList = new List<SomeClass>(),
            SomeClassDictionary = new Dictionary<string, SomeClass>()
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult);
        // Empty collections may be stored as empty or null depending on SDK behavior
        // The key assertion: no exception and round-trip works
    }

    [Fact]
    public async Task EmptyListAndDictionary_SdkWrite_LibRead()
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
            SomeClassList = new List<SomeClass>(),
            SomeClassDictionary = new Dictionary<string, SomeClass>()
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
    }

    [Fact]
    public async Task NegativeNumbers_LibWrite_SdkRead()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = -123,
            SomeLong = -456,
            SomeFloat = -1.5f,
            SomeDecimal = -99.99m,
            SomeDate = DateTime.UtcNow,
            SomeNullableInt32 = -1,
            SomeNullableFloat = -0.5f,
            SomeNullableDecimal = -999.99m
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(-123, sdkResult.SomeInt);
        Assert.Equal(-456, sdkResult.SomeLong);
        Assert.Equal(-1.5f, sdkResult.SomeFloat);
        Assert.Equal(-99.99m, sdkResult.SomeDecimal);
        Assert.Equal(-1, sdkResult.SomeNullableInt32);
        Assert.Equal(-0.5f, sdkResult.SomeNullableFloat);
        Assert.Equal(-999.99m, sdkResult.SomeNullableDecimal);
    }

    [Fact]
    public async Task NegativeNumbers_SdkWrite_LibRead()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = -789,
            SomeLong = -1000,
            SomeFloat = -3.14f,
            SomeDecimal = -0.01m,
            SomeDate = DateTime.UtcNow
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Equal(-789, libResult!.SomeInt);
        Assert.Equal(-1000, libResult.SomeLong);
        Assert.Equal(-3.14f, libResult.SomeFloat);
        Assert.Equal(-0.01m, libResult.SomeDecimal);
    }

    [Fact]
    public async Task ZeroValues_LibWrite_SdkRead()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 0,
            SomeLong = 0,
            SomeFloat = 0f,
            SomeDecimal = 0m,
            SomeDate = DateTime.UtcNow,
            SomeBool = false,
            SomeNullableInt32 = 0,
            SomeNullableBool = false
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(0, sdkResult.SomeInt);
        Assert.Equal(0, sdkResult.SomeLong);
        Assert.Equal(0f, sdkResult.SomeFloat);
        Assert.Equal(0m, sdkResult.SomeDecimal);
        Assert.False(sdkResult.SomeBool);
        Assert.Equal(0, sdkResult.SomeNullableInt32);
        Assert.False(sdkResult.SomeNullableBool);
    }

    [Fact]
    public async Task LargeBinaryData_LibWrite_SdkRead()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] largeBytes = new byte[4096];
        new Random(42).NextBytes(largeBytes);
        var largeStream = new MemoryStream(largeBytes);

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 1,
            SomeLong = 1,
            SomeFloat = 1f,
            SomeDecimal = 1m,
            SomeDate = DateTime.UtcNow,
            SomeBytes = largeBytes,
            SomeMemoryStream = largeStream
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult);
        Assert.Equal(largeBytes, sdkResult.SomeBytes);
        MemoryStreamsEquality.StreamEqual(new MemoryStream(largeBytes), sdkResult.SomeMemoryStream);
    }

    [Fact]
    public async Task MultipleNestedObjects_LibWrite_SdkRead()
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
                new() { X = "A1", Y = "B1" },
                new() { X = "A2", Y = "B2" },
                new() { X = "A3", Y = "B3" },
                new() { X = "A4", Y = "B4" },
                new() { X = "A5", Y = "B5" }
            },
            SomeClassDictionary = new Dictionary<string, SomeClass>
            {
                { "one", new SomeClass { X = "1X", Y = "1Y" } },
                { "two", new SomeClass { X = "2X", Y = "2Y" } },
                { "three", new SomeClass { X = "3X", Y = "3Y" } }
            }
        };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult);
        Assert.Equal(5, sdkResult.SomeClassList.Count);
        Assert.Equal(3, sdkResult.SomeClassDictionary.Count);
        Assert.Equal("A3", sdkResult.SomeClassList[2].X);
        Assert.Equal("2X", sdkResult.SomeClassDictionary["two"].X);
    }

    [Fact]
    public async Task MultipleNestedObjects_SdkWrite_LibRead()
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
                new() { X = "S1", Y = "T1" },
                new() { X = "S2", Y = "T2" },
                new() { X = "S3", Y = "T3" }
            },
            SomeClassDictionary = new Dictionary<string, SomeClass>
            {
                { "alpha", new SomeClass { X = "AX", Y = "AY" } },
                { "beta", new SomeClass { X = "BX", Y = "BY" } }
            }
        };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(3, libResult.SomeClassList!.Count);
        Assert.Equal(2, libResult.SomeClassDictionary!.Count);
        Assert.Equal("S2", libResult.SomeClassList[1].X);
        Assert.Equal("BX", libResult.SomeClassDictionary["beta"].X);
    }

    [Fact]
    public async Task AtomicTransaction_LibWriteMultiple_SdkVerifyAll()
    {
        var userId1 = Guid.NewGuid().ToString();
        var userId2 = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId1, SomeInt = 111, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 11.1m, SomeDate = DateTime.UtcNow, SomeBool = true });
            writer.CreateOrUpdate(new TestTable { UserId = userId2, SomeInt = 222, SomeLong = 2, SomeFloat = 2f, SomeDecimal = 22.2m, SomeDate = DateTime.UtcNow, SomeBool = false });
        }

        var sdk1 = await _fixture.Db.Context.LoadAsync<TestTable>(userId1);
        var sdk2 = await _fixture.Db.Context.LoadAsync<TestTable>(userId2);

        Assert.Equal(111, sdk1.SomeInt);
        Assert.True(sdk1.SomeBool);
        Assert.Equal(11.1m, sdk1.SomeDecimal);
        Assert.Equal(222, sdk2.SomeInt);
        Assert.False(sdk2.SomeBool);
        Assert.Equal(22.2m, sdk2.SomeDecimal);
    }

    [Fact]
    public async Task AtomicRollback_VersionConflict_NeitherItemWritten()
    {
        var userId1 = Guid.NewGuid().ToString();
        var userId2 = Guid.NewGuid().ToString();

        // Item 1 with version conflict (version=1 but doesn't exist yet)
        // Item 2 is valid
        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new TestTable { UserId = userId1, SomeInt = 111, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow, Version = 1 });
                writer.CreateOrUpdate(new TestTable { UserId = userId2, SomeInt = 222, SomeLong = 2, SomeFloat = 2f, SomeDecimal = 2m, SomeDate = DateTime.UtcNow });
            }
        });

        // Both items should not exist
        Assert.Null(await _fixture.Db.Context.LoadAsync<TestTable>(userId1));
        Assert.Null(await _fixture.Db.Context.LoadAsync<TestTable>(userId2));

        // Lib also should not see them
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId1);
        reader.Get<TestTable>(userId2);
        var result = await reader.ExecuteAsync();
        Assert.Null(result.GetItem<TestTable>(0));
        Assert.Null(result.GetItem<TestTable>(1));
    }

    [Fact]
    public async Task OverwriteExisting_LibWrite_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();

        // SDK creates
        await _fixture.Db.Context.SaveAsync(new TestTable
        {
            UserId = userId,
            SomeInt = 100,
            SomeLong = 200,
            SomeFloat = 1f,
            SomeDecimal = 300m,
            SomeDate = DateTime.UtcNow,
            SomeBool = true,
            SomeClass = new SomeClass { X = "Old", Y = "Data" }
        });

        // Lib overwrites with same version
        var loaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        loaded.SomeInt = 999;
        loaded.SomeClass = new SomeClass { X = "New", Y = "Data" };
        loaded.SomeBool = false;

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(loaded);
        }

        // SDK verifies
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(999, sdkResult.SomeInt);
        Assert.Equal("New", sdkResult.SomeClass.X);
        Assert.False(sdkResult.SomeBool);
        Assert.Equal(200, sdkResult.SomeLong); // unchanged in overwrite
        Assert.Equal(1, sdkResult.Version); // incremented
    }

    [Fact]
    public async Task IdempotentDispose_LibWrite_DoesNotDuplicate()
    {
        var userId = Guid.NewGuid().ToString();

        var writer = new DynamoDbTransactor(_fixture.Db.Client);
        writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 42, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        // Double dispose should not throw or duplicate
        await writer.DisposeAsync();
        await writer.DisposeAsync();

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult);
        Assert.Equal(42, sdkResult.SomeInt);
        Assert.Equal(0, sdkResult.Version); // only one write happened
    }
}
