using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests for delete operations: verifies that deleting via our library
/// removes the same data that the SDK confirms is gone.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonDeleteTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonDeleteTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Delete_ByKeyNameValue_SdkWrite_LibDelete_SdkVerifyGone()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.DeleteAsync<TestTable>(nameof(TestTable.UserId), userId);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Null(sdkResult);
    }

    [Fact]
    public async Task Delete_ByExpression_SdkWrite_LibDelete_SdkVerifyGone()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.DeleteAsync<TestTable, string>(t => t.UserId, userId);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Null(sdkResult);
    }

    [Fact]
    public async Task Delete_ByHashKey_SdkWrite_LibDelete_SdkVerifyGone()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.DeleteAsync<TestTable>(userId);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Null(sdkResult);
    }

    [Fact]
    public async Task Delete_LibWrite_LibDelete_SdkVerifyGone()
    {
        var userId = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 42, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        }

        // Verify it exists via SDK
        var beforeDelete = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.NotNull(beforeDelete);

        // Delete via lib
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.DeleteAsync<TestTable>(userId);
        }

        // Verify gone via SDK
        var afterDelete = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Null(afterDelete);
    }

    [Fact]
    public async Task Delete_LibWrite_LibDelete_LibVerifyGone()
    {
        var userId = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 42, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        }

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.DeleteAsync<TestTable>(userId);
        }

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        Assert.Null(result.GetItem<TestTable>(0));
    }

    [Fact]
    public async Task Delete_NonExistent_BothSystemsDoNotThrow()
    {
        var userId = Guid.NewGuid().ToString();

        // Delete via lib — should not throw
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.DeleteAsync<TestTable>(userId);
        }

        // Verify via SDK — should return null
        var sdkResult = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        Assert.Null(sdkResult);

        // Verify via lib — should return null
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<TestTable>(userId);
        var result = await reader.ExecuteAsync();
        Assert.Null(result.GetItem<TestTable>(0));
    }

    [Fact]
    public async Task Delete_CompositeKey_SdkWrite_LibDelete_SdkVerifyGone()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "del-comp";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Status = "ToDelete", Amount = 50m });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            var deleteRequest = new DeleteTransactionRequest<CompositeKeyTestTable>(pk, sk);
            writer.AddRawRequest(deleteRequest);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, sk);
        Assert.Null(sdkResult);
    }

    [Fact]
    public async Task Delete_OnlyTargetItem_OthersRemain()
    {
        var userId1 = Guid.NewGuid().ToString();
        var userId2 = Guid.NewGuid().ToString();

        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId1, SomeInt = 111, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId2, SomeInt = 222, SomeLong = 2, SomeFloat = 2f, SomeDecimal = 2m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.DeleteAsync<TestTable>(userId1);
        }

        Assert.Null(await _fixture.Db.Context.LoadAsync<TestTable>(userId1));
        Assert.NotNull(await _fixture.Db.Context.LoadAsync<TestTable>(userId2));
        Assert.Equal(222, (await _fixture.Db.Context.LoadAsync<TestTable>(userId2)).SomeInt);
    }
}
