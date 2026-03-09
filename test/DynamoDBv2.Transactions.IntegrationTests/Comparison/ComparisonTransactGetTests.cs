using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests for TransactGetItems (our lib) vs DynamoDBContext.LoadAsync (SDK):
/// verifies both read paths return identical data for the same DynamoDB item.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonTransactGetTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonTransactGetTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SingleItem_TransactGet_MatchesSdkLoad()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [5, 10, 15];
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 42,
            SomeNullableInt32 = 7,
            SomeLong = 100,
            SomeNullableLong = 200,
            SomeFloat = 3.14f,
            SomeNullableFloat = 2.71f,
            SomeDecimal = 99.99m,
            SomeNullableDecimal = 88.88m,
            SomeDate = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc),
            SomeNullableDate1 = new DateTime(2025, 6, 14, 12, 0, 0, DateTimeKind.Utc),
            SomeBool = true,
            SomeNullableBool = false,
            SomeClass = new SomeClass { X = "CX", Y = "CY" },
            SomeRecord = new SomeRecord { X = "RX", Y = "RY" },
            SomeClassList = new List<SomeClass> { new() { X = "LX", Y = "LY" } },
            SomeClassDictionary = new Dictionary<string, SomeClass> { { "K", new SomeClass { X = "DX", Y = "DY" } } },
            SomeBytes = bytes,
            SomeMemoryStream = new MemoryStream(bytes)
        };

        // Write via lib
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        // Read via SDK
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        // Read via lib TransactGet
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        // Compare all fields
        Assert.NotNull(sdkResult);
        Assert.NotNull(libResult);
        Assert.Equal(sdkResult.UserId, libResult.UserId);
        Assert.Equal(sdkResult.SomeInt, libResult.SomeInt);
        Assert.Equal(sdkResult.SomeNullableInt32, libResult.SomeNullableInt32);
        Assert.Equal(sdkResult.SomeLong, libResult.SomeLong);
        Assert.Equal(sdkResult.SomeNullableLong, libResult.SomeNullableLong);
        Assert.Equal(sdkResult.SomeFloat, libResult.SomeFloat);
        Assert.Equal(sdkResult.SomeNullableFloat, libResult.SomeNullableFloat);
        Assert.Equal(sdkResult.SomeDecimal, libResult.SomeDecimal);
        Assert.Equal(sdkResult.SomeNullableDecimal, libResult.SomeNullableDecimal);
        Assert.Equal(sdkResult.SomeDate, libResult.SomeDate, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
        Assert.Equal(sdkResult.SomeNullableDate1!.Value, libResult.SomeNullableDate1!.Value, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
        Assert.Equal(sdkResult.SomeBool, libResult.SomeBool);
        Assert.Equal(sdkResult.SomeNullableBool, libResult.SomeNullableBool);
        Assert.Equal(sdkResult.SomeClass?.X, libResult.SomeClass?.X);
        Assert.Equal(sdkResult.SomeClass?.Y, libResult.SomeClass?.Y);
        Assert.Equal(sdkResult.SomeRecord?.X, libResult.SomeRecord?.X);
        Assert.Equal(sdkResult.SomeRecord?.Y, libResult.SomeRecord?.Y);
        Assert.Equal(sdkResult.SomeClassList?[0].X, libResult.SomeClassList?[0].X);
        Assert.Equal(sdkResult.SomeClassDictionary?["K"].X, libResult.SomeClassDictionary?["K"].X);
        Assert.Equal(sdkResult.SomeBytes, libResult.SomeBytes);
        Assert.Equal(sdkResult.Version, libResult.Version);
    }

    [Fact]
    public async Task MultipleItems_TransactGet_MatchSdkLoads()
    {
        var userId1 = Guid.NewGuid().ToString();
        var userId2 = Guid.NewGuid().ToString();
        var userId3 = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId1, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
            writer.CreateOrUpdate(new TestTable { UserId = userId2, SomeInt = 2, SomeLong = 2, SomeFloat = 2f, SomeDecimal = 2m, SomeDate = DateTime.UtcNow });
            writer.CreateOrUpdate(new TestTable { UserId = userId3, SomeInt = 3, SomeLong = 3, SomeFloat = 3f, SomeDecimal = 3m, SomeDate = DateTime.UtcNow });
        }

        // SDK reads
        var sdk1 = await _fixture.Db.Context.LoadAsync<TestTable>(userId1);
        var sdk2 = await _fixture.Db.Context.LoadAsync<TestTable>(userId2);
        var sdk3 = await _fixture.Db.Context.LoadAsync<TestTable>(userId3);

        // Lib TransactGet reads
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId1);
        reader.Get<TestTable>(userId2);
        reader.Get<TestTable>(userId3);
        var result = await reader.ExecuteAsync();

        var lib1 = result.GetItem<TestTable>(0);
        var lib2 = result.GetItem<TestTable>(1);
        var lib3 = result.GetItem<TestTable>(2);

        Assert.Equal(sdk1.SomeInt, lib1!.SomeInt);
        Assert.Equal(sdk2.SomeInt, lib2!.SomeInt);
        Assert.Equal(sdk3.SomeInt, lib3!.SomeInt);
        Assert.Equal(sdk1.Version, lib1.Version);
    }

    [Fact]
    public async Task NonExistentItem_BothReturnNull()
    {
        var userId = Guid.NewGuid().ToString();

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Null(sdkResult);
        Assert.Null(libResult);
    }

    [Fact]
    public async Task MixExistingAndNonExistent_BothAgree()
    {
        var existingId = Guid.NewGuid().ToString();
        var nonExistentId = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = existingId, SomeInt = 777, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        }

        // SDK
        var sdkExisting = await _fixture.Db.Context.LoadAsync<TestTable>(existingId);
        var sdkMissing = await _fixture.Db.Context.LoadAsync<TestTable>(nonExistentId);

        // Lib
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(existingId);
        reader.Get<TestTable>(nonExistentId);
        var result = await reader.ExecuteAsync();

        Assert.NotNull(sdkExisting);
        Assert.Null(sdkMissing);
        Assert.NotNull(result.GetItem<TestTable>(0));
        Assert.Null(result.GetItem<TestTable>(1));
        Assert.Equal(sdkExisting.SomeInt, result.GetItem<TestTable>(0)!.SomeInt);
    }

    [Fact]
    public async Task AfterUpdate_TransactGet_MatchesSdkLoad()
    {
        var userId = Guid.NewGuid().ToString();

        // Create
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 100, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        }

        // Update
        var loaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        loaded.SomeInt = 200;
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(loaded);
        }

        // Compare both read paths
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Equal(200, sdkResult.SomeInt);
        Assert.Equal(200, libResult!.SomeInt);
        Assert.Equal(sdkResult.Version, libResult.Version);
    }

    [Fact]
    public async Task WithProjection_ReturnsOnlyProjectedFields()
    {
        var userId = Guid.NewGuid().ToString();
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable
            {
                UserId = userId,
                SomeInt = 42,
                SomeBool = true,
                SomeFloat = 3.14f,
                SomeDecimal = 99.99m,
                SomeLong = 1,
                SomeDate = DateTime.UtcNow
            });
        }

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId, x => new { x.SomeInt, x.SomeBool });
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(42, libResult.SomeInt);
        Assert.True(libResult.SomeBool);
        Assert.Equal(default(float), libResult.SomeFloat);
        Assert.Equal(default(decimal), libResult.SomeDecimal);
    }

    [Fact]
    public async Task CompositeKey_TransactGet_MatchesSdkLoad()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "tg-comp";

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "Active", Amount = 150m });
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<CompositeKeyTestTable>(pk, sk);
        var libResult = (await reader.ExecuteAsync()).GetItem<CompositeKeyTestTable>(0);

        Assert.NotNull(sdkResult);
        Assert.NotNull(libResult);
        Assert.Equal(sdkResult.PartitionKey, libResult.PartitionKey);
        Assert.Equal(sdkResult.SortKey, libResult.SortKey);
        Assert.Equal(sdkResult.Status, libResult.Status);
        Assert.Equal(sdkResult.Amount, libResult.Amount);
        Assert.Equal(sdkResult.Version, libResult.Version);
    }

    [Fact]
    public async Task Version_TransactGet_MatchesSdkLoad()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        // Update twice to get version=2
        for (int i = 0; i < 2; i++)
        {
            var loaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            loaded.SomeInt += 1;
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(loaded);
            }
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Equal(2, sdkResult.Version);
        Assert.Equal(2, libResult!.Version);
        Assert.Equal(sdkResult.SomeInt, libResult.SomeInt);
    }

    [Fact]
    public async Task NullablePropertiesNull_TransactGet_MatchesSdkLoad()
    {
        var userId = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable
            {
                UserId = userId,
                SomeInt = 1,
                SomeLong = 1,
                SomeFloat = 1f,
                SomeDecimal = 1m,
                SomeDate = DateTime.UtcNow,
                SomeNullableInt32 = null,
                SomeNullableBool = null,
                SomeClass = null
            });
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Null(sdkResult.SomeNullableInt32);
        Assert.Null(libResult!.SomeNullableInt32);
        Assert.Null(sdkResult.SomeNullableBool);
        Assert.Null(libResult.SomeNullableBool);
        Assert.Null(sdkResult.SomeClass);
        Assert.Null(libResult.SomeClass);
    }

    [Fact]
    public async Task AfterPatch_TransactGet_MatchesSdkLoad()
    {
        var userId = Guid.NewGuid().ToString();

        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 10, SomeLong = 20, SomeFloat = 1f, SomeDecimal = 30m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, int>(userId, t => t.SomeInt, 999);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Equal(999, sdkResult.SomeInt);
        Assert.Equal(999, libResult!.SomeInt);
        Assert.Equal(sdkResult.SomeLong, libResult.SomeLong);
        Assert.Equal(sdkResult.SomeDecimal, libResult.SomeDecimal);
    }

    [Fact]
    public async Task SdkWritten_TransactGet_MatchesSdkLoad()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [42, 43, 44];

        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 555,
            SomeLong = 666,
            SomeFloat = 7.77f,
            SomeDecimal = 888.88m,
            SomeDate = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            SomeBool = true,
            SomeClass = new SomeClass { X = "A", Y = "B" },
            SomeBytes = bytes
        };

        await _fixture.Db.Context.SaveAsync(item);

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Equal(sdkResult.SomeInt, libResult!.SomeInt);
        Assert.Equal(sdkResult.SomeLong, libResult.SomeLong);
        Assert.Equal(sdkResult.SomeFloat, libResult.SomeFloat);
        Assert.Equal(sdkResult.SomeDecimal, libResult.SomeDecimal);
        Assert.Equal(sdkResult.SomeBool, libResult.SomeBool);
        Assert.Equal(sdkResult.SomeClass?.X, libResult.SomeClass?.X);
        Assert.Equal(sdkResult.SomeBytes, libResult.SomeBytes);
        Assert.Equal(sdkResult.Version, libResult.Version);
    }
}
