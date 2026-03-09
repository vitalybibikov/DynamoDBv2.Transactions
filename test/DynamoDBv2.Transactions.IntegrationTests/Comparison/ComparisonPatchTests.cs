using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests for patch operations: verifies that patching via our library
/// produces the same DynamoDB state that the SDK reads back correctly.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonPatchTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonPatchTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Patch_Int_ByExpression_LibPatch_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 100, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, int>(userId, t => t.SomeInt, 999);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(999, sdkResult.SomeInt);
    }

    [Fact]
    public async Task Patch_Decimal_ByExpression_LibPatch_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 100.50m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, decimal>(userId, t => t.SomeDecimal, 999.99m);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(999.99m, sdkResult.SomeDecimal);
    }

    [Fact]
    public async Task Patch_Float_ByExpression_LibPatch_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1.5f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, float>(userId, t => t.SomeFloat, 99.9f);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(99.9f, sdkResult.SomeFloat);
    }

    [Fact]
    public async Task Patch_Bool_ByExpression_LibPatch_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow, SomeBool = false });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, bool>(userId, t => t.SomeBool, true);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.True(sdkResult.SomeBool);
    }

    [Fact]
    public async Task Patch_NullableToValue_LibPatch_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow, SomeNullableInt32 = null });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, int?>(userId, t => t.SomeNullableInt32, 42);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(42, sdkResult.SomeNullableInt32);
    }

    [Fact]
    public async Task Patch_ValueToNull_LibPatch_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow, SomeNullableInt32 = 42 });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, int?>(userId, t => t.SomeNullableInt32, null);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Null(sdkResult.SomeNullableInt32);
    }

    [Fact]
    public async Task Patch_ByModelAndPropertyName_LibPatch_SdkVerify()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable { UserId = userId, SomeInt = 100, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 50m, SomeDate = DateTime.UtcNow };
        await _fixture.Db.Context.SaveAsync(item);

        item.SomeDecimal = 999m;
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync(item, nameof(item.SomeDecimal));
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(999m, sdkResult.SomeDecimal);
        Assert.Equal(100, sdkResult.SomeInt); // unchanged
    }

    [Fact]
    public async Task Patch_DoesNotAffectOtherProperties()
    {
        var userId = Guid.NewGuid().ToString();
        byte[] bytes = [1, 2, 3];
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 111,
            SomeLong = 222,
            SomeFloat = 3.14f,
            SomeDecimal = 99.99m,
            SomeBool = true,
            SomeDate = new DateTime(2025, 6, 15, 10, 0, 0, DateTimeKind.Utc),
            SomeClass = new SomeClass { X = "OrigX", Y = "OrigY" },
            SomeBytes = bytes
        };
        await _fixture.Db.Context.SaveAsync(item);

        // Patch only SomeInt
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, int>(userId, t => t.SomeInt, 999);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(999, sdkResult.SomeInt);
        Assert.Equal(222, sdkResult.SomeLong);
        Assert.Equal(3.14f, sdkResult.SomeFloat);
        Assert.Equal(99.99m, sdkResult.SomeDecimal);
        Assert.True(sdkResult.SomeBool);
        Assert.Equal("OrigX", sdkResult.SomeClass.X);
        Assert.Equal(bytes, sdkResult.SomeBytes);
    }

    [Fact]
    public async Task Patch_SdkWrite_LibPatch_SdkVerify_EndToEnd()
    {
        var userId = Guid.NewGuid().ToString();
        var item = new TestTable
        {
            UserId = userId,
            SomeInt = 10,
            SomeLong = 20,
            SomeFloat = 1f,
            SomeDecimal = 100m,
            SomeDate = DateTime.UtcNow,
            SomeBool = false
        };
        await _fixture.Db.Context.SaveAsync(item);

        // Patch multiple fields sequentially
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, int>(userId, t => t.SomeInt, 50);
        }

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, bool>(userId, t => t.SomeBool, true);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Equal(50, sdkResult.SomeInt);
        Assert.True(sdkResult.SomeBool);
        Assert.Equal(20, sdkResult.SomeLong); // unchanged
        Assert.Equal(100m, sdkResult.SomeDecimal); // unchanged
    }

    [Fact]
    public async Task Patch_LibPatch_LibRead_SdkRead_AllAgree()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 10, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 50m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<TestTable, decimal>(userId, t => t.SomeDecimal, 777m);
        }

        // Read via SDK
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);

        // Read via lib
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var libResult = (await reader.ExecuteAsync()).GetItem<TestTable>(0);

        Assert.Equal(777m, sdkResult.SomeDecimal);
        Assert.Equal(777m, libResult!.SomeDecimal);
        Assert.Equal(sdkResult.SomeInt, libResult.SomeInt);
    }

    [Fact]
    public async Task Patch_CompositeKey_LibPatch_SdkVerify()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "patch-comp";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "Original", Amount = 100m });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            var patchRequest = new PatchTransactionRequest<CompositeKeyTestTable>(
                pk, sk,
                new Property { Name = "Status", Value = "Patched" });
            writer.AddRawRequest(patchRequest);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Equal("Patched", sdkResult.Status);
        Assert.Equal(100m, sdkResult.Amount); // unchanged
    }
}
