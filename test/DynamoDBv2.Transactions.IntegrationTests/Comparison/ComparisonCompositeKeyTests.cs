using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using Amazon.DynamoDBv2.Model;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests for composite key (hash + range) operations:
/// verifies that our library handles composite keys identically to the AWS SDK.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonCompositeKeyTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonCompositeKeyTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task CompositeKey_LibWrite_SdkRead_AllProperties()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "item-001";

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new CompositeKeyTestTable
            {
                PartitionKey = pk,
                SortKey = sk,
                Status = "Active",
                Amount = 99.99m,
                IsActive = true
            });
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.NotNull(sdkResult);
        Assert.Equal(pk, sdkResult.PartitionKey);
        Assert.Equal(sk, sdkResult.SortKey);
        Assert.Equal("Active", sdkResult.Status);
        Assert.Equal(99.99m, sdkResult.Amount);
        Assert.True(sdkResult.IsActive);
        Assert.Equal(0, sdkResult.Version);
    }

    [Fact]
    public async Task CompositeKey_SdkWrite_LibRead_AllProperties()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "item-002";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable
        {
            PartitionKey = pk,
            SortKey = sk,
            Status = "Pending",
            Amount = 50.50m,
            IsActive = false
        });

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<CompositeKeyTestTable>(pk, sk);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<CompositeKeyTestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(pk, libResult.PartitionKey);
        Assert.Equal(sk, libResult.SortKey);
        Assert.Equal("Pending", libResult.Status);
        Assert.Equal(50.50m, libResult.Amount);
        Assert.False(libResult.IsActive);
        Assert.Equal(0, libResult.Version);
    }

    [Fact]
    public async Task CompositeKey_MultipleItemsSameHash_LibWrite_SdkRead()
    {
        var pk = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = "a", Status = "First", Amount = 10m });
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = "b", Status = "Second", Amount = 20m });
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = "c", Status = "Third", Amount = 30m });
        }

        Assert.Equal("First", (await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "a")).Status);
        Assert.Equal("Second", (await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "b")).Status);
        Assert.Equal("Third", (await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "c")).Status);
    }

    [Fact]
    public async Task CompositeKey_MultipleItemsSameHash_SdkWrite_LibRead()
    {
        var pk = Guid.NewGuid().ToString();

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = "x", Status = "Alpha", Amount = 1m });
        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = "y", Status = "Beta", Amount = 2m });

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<CompositeKeyTestTable>(pk, "x");
        reader.Get<CompositeKeyTestTable>(pk, "y");
        var result = await reader.ExecuteAsync();

        Assert.Equal("Alpha", result.GetItem<CompositeKeyTestTable>(0)!.Status);
        Assert.Equal("Beta", result.GetItem<CompositeKeyTestTable>(1)!.Status);
    }

    [Fact]
    public async Task CompositeKey_Patch_LibPatch_SdkVerify()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "patch-test";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "Original", Amount = 100m, IsActive = false });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.AddRawRequest(new PatchTransactionRequest<CompositeKeyTestTable>(
                pk, sk, new Property { Name = "Status", Value = "Updated" }));
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Equal("Updated", sdkResult.Status);
        Assert.Equal(100m, sdkResult.Amount); // unchanged
    }

    [Fact]
    public async Task CompositeKey_Delete_LibDelete_SdkVerifyGone()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "delete-test";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "ToDelete" });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.AddRawRequest(new DeleteTransactionRequest<CompositeKeyTestTable>(pk, sk));
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Null(sdkResult);
    }

    [Fact]
    public async Task CompositeKey_Delete_OnlyTargetItem_OthersRemain()
    {
        var pk = Guid.NewGuid().ToString();

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = "keep", Status = "Keep" });
        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = "remove", Status = "Remove" });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.AddRawRequest(new DeleteTransactionRequest<CompositeKeyTestTable>(pk, "remove"));
        }

        Assert.NotNull(await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "keep"));
        Assert.Null(await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "remove"));
    }

    [Fact]
    public async Task CompositeKey_ConditionEquals_LibCheck_SdkVerifyState()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "cond-eq";

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Amount = 200m });
        }

        // Condition check passes (no exception)
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionEquals<CompositeKeyTestTable, decimal>(pk, sk, x => x.Amount, 200m);
        }

        // SDK confirms value is still 200m
        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Equal(200m, sdkResult.Amount);
    }

    [Fact]
    public async Task CompositeKey_ConditionEquals_Fails_Throws()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "cond-fail";

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Amount = 200m });
        }

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<CompositeKeyTestTable, decimal>(pk, sk, x => x.Amount, 999m);
            }
        });
    }

    [Fact]
    public async Task CompositeKey_VersionIncrement_LibWrite_SdkRead()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "ver-inc";

        // Write v0
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "v0" });
        }

        var loaded = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Equal(0, loaded.Version);

        // Update to v1
        loaded.Status = "v1";
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(loaded);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Equal(1, sdkResult.Version);
        Assert.Equal("v1", sdkResult.Status);
    }

    [Fact]
    public async Task CompositeKey_VersionIncrement_SdkWrite_LibRead()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "ver-sdk";

        var item = new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "v0" };
        await _fixture.Db.Context.SaveAsync(item);
        Assert.Equal(0, item.Version);

        item.Status = "v1";
        await _fixture.Db.Context.SaveAsync(item);
        Assert.Equal(1, item.Version);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<CompositeKeyTestTable>(pk, sk);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<CompositeKeyTestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(1, libResult.Version);
        Assert.Equal("v1", libResult.Status);
    }

    [Fact]
    public async Task CompositeKey_NonExistent_BothReturnNull()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "nonexistent";

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Null(sdkResult);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<CompositeKeyTestTable>(pk, sk);
        var result = await reader.ExecuteAsync();
        Assert.Null(result.GetItem<CompositeKeyTestTable>(0));
    }

    [Fact]
    public async Task CompositeKey_BidirectionalRoundTrip()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "roundtrip";

        // Lib writes
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "LibCreated", Amount = 75m, IsActive = true });
        }

        // SDK reads and updates
        var sdkLoaded = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Equal("LibCreated", sdkLoaded.Status);
        sdkLoaded.Status = "SdkUpdated";
        await _fixture.Db.Context.SaveAsync(sdkLoaded);

        // Lib reads updated value
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<CompositeKeyTestTable>(pk, sk);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<CompositeKeyTestTable>(0);

        Assert.Equal("SdkUpdated", libResult!.Status);
        Assert.Equal(75m, libResult.Amount);
        Assert.True(libResult.IsActive);
    }

    [Fact]
    public async Task CompositeKey_Projection_LibRead_MatchesExpected()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "proj";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "Active", Amount = 200m, IsActive = true });

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<CompositeKeyTestTable>(pk, sk, x => new { x.Status });
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<CompositeKeyTestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal("Active", libResult.Status);
        Assert.Equal(default(decimal), libResult.Amount); // not projected
    }
}
