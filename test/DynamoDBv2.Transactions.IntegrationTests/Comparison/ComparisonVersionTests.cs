using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Amazon.DynamoDBv2.Model;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests for version semantics: both systems must agree on version=0 for new items,
/// version increments, and version mismatch behavior.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonVersionTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonVersionTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task NewItem_LibWrite_SdkRead_VersionIsZero()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(sdkResult);
        Assert.Equal(0, sdkResult.Version);
    }

    [Fact]
    public async Task NewItem_SdkWrite_LibRead_VersionIsZero()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow };

        await _fixture.Db.Context.SaveAsync(item);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(0, libResult.Version);
    }

    [Fact]
    public async Task VersionIncrement_LibWriteTwice_SdkReadsVersionOne()
    {
        var userId = Guid.NewGuid().ToString();

        // First write (version=null → stored as 0)
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        }

        // Load to get version=0
        var loaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(0, loaded.Version);

        // Second write with version=0 → stored as 1
        loaded.SomeInt = 2;
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(loaded);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(1, sdkResult.Version);
        Assert.Equal(2, sdkResult.SomeInt);
    }

    [Fact]
    public async Task VersionIncrement_SdkWriteTwice_LibReadsVersionOne()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 10, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow };

        await _fixture.Db.Context.SaveAsync(item);
        Assert.Equal(0, item.Version); // SDK updates in-place

        item.SomeInt = 20;
        await _fixture.Db.Context.SaveAsync(item);
        Assert.Equal(1, item.Version);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(1, libResult.Version);
        Assert.Equal(20, libResult.SomeInt);
    }

    [Fact]
    public async Task VersionMismatch_LibWrite_ThrowsTransactionCanceled()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow };

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        // Attempt to write with wrong version
        var staleItem = new TestTable { UserId = userId, SomeInt = 999, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow, Version = 99 };

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(staleItem);
            }
        });

        // Verify original data is unchanged
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(0, sdkResult.Version);
        Assert.Equal(1, sdkResult.SomeInt);
    }

    [Fact]
    public async Task MultipleUpdates_VersionIncrements_BothSystemsAgree()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 0, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow };

        // Write v0 via lib
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        for (int expectedVersion = 0; expectedVersion < 3; expectedVersion++)
        {
            // Read via SDK
            var sdkLoaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(expectedVersion, sdkLoaded.Version);

            // Read via lib
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(userId);
            var libLoaded = (await reader.ExecuteAsync()).GetItem<TestTable>(0);
            Assert.Equal(expectedVersion, libLoaded!.Version);

            // Both agree on version
            Assert.Equal(sdkLoaded.Version, libLoaded.Version);

            // Update for next iteration (use SDK-loaded to get correct version)
            sdkLoaded.SomeInt = expectedVersion + 1;
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(sdkLoaded);
            }
        }
    }

    [Fact]
    public async Task CrossSystemVersioning_LibWriteV0_SdkUpdateV1_LibReadsV1()
    {
        var userId = Guid.NewGuid().ToString();

        // Lib writes v0
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 100, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        }

        // SDK updates to v1
        var sdkLoaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        sdkLoaded.SomeInt = 200;
        await _fixture.Db.Context.SaveAsync(sdkLoaded);

        // Lib reads v1
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(1, libResult.Version);
        Assert.Equal(200, libResult.SomeInt);
    }

    [Fact]
    public async Task CrossSystemVersioning_SdkWriteV0_LibUpdateV1_SdkReadsV1()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 100, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow };

        // SDK writes v0
        await _fixture.Db.Context.SaveAsync(item);
        Assert.Equal(0, item.Version);

        // Lib updates to v1
        item.SomeInt = 300;
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(item);
        }

        // SDK reads v1
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(1, sdkResult.Version);
        Assert.Equal(300, sdkResult.SomeInt);
    }
}
