using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for composite key support on Delete, ConditionCheck, and Patch operations.
/// Uses OrderTestEntity (hash + range) and ProductTestEntity (hash only).
/// </summary>
public class CompositeKeyWriteTests
{
    // ──────────────────────────────────────────────
    //  Delete — composite key
    // ──────────────────────────────────────────────

    [Fact]
    public void DeleteTransactionRequest_CompositeKey_SetsBothKeys()
    {
        var request = new DeleteTransactionRequest<OrderTestEntity>("ord-1", "sk-1");

        Assert.Equal(2, request.Key.Count);
        Assert.Equal("ord-1", request.Key["order_id"].S);
        Assert.Equal("sk-1", request.Key["sort_key"].S);
    }

    [Fact]
    public void DeleteTransactionRequest_CompositeKey_GetOperation_HasBothKeys()
    {
        var request = new DeleteTransactionRequest<OrderTestEntity>("ord-1", "sk-1");

        var operation = request.GetOperation();

        Assert.NotNull(operation.DeleteType);
        var delete = operation.DeleteType!;
        Assert.Equal("Orders", delete.TableName);
        Assert.Equal(2, delete.Key.Count);
        Assert.Equal("ord-1", delete.Key["order_id"].S);
        Assert.Equal("sk-1", delete.Key["sort_key"].S);
    }

    // ──────────────────────────────────────────────
    //  ConditionCheck — composite key
    // ──────────────────────────────────────────────

    [Fact]
    public void ConditionCheckTransactionRequest_CompositeKey_SetsBothKeys()
    {
        var request = new ConditionCheckTransactionRequest<OrderTestEntity>("ord-1", "sk-1");

        Assert.Equal(2, request.Key.Count);
        Assert.Equal("ord-1", request.Key["order_id"].S);
        Assert.Equal("sk-1", request.Key["sort_key"].S);
    }

    [Fact]
    public void ConditionCheckTransactionRequest_CompositeKey_WithCondition_Works()
    {
        var request = new ConditionCheckTransactionRequest<OrderTestEntity>("ord-1", "sk-1");
        request.Equals<OrderTestEntity, string>(x => x.Status, "Active");

        var operation = request.GetOperation();

        Assert.NotNull(operation.ConditionCheckType);
        var check = operation.ConditionCheckType!;

        // Key
        Assert.Equal(2, check.Key.Count);
        Assert.Equal("ord-1", check.Key["order_id"].S);
        Assert.Equal("sk-1", check.Key["sort_key"].S);

        // Condition expression should contain the equality check
        Assert.NotNull(check.ConditionExpression);
        Assert.Contains("=", check.ConditionExpression);

        // Expression attribute names/values should be populated
        Assert.NotEmpty(check.ExpressionAttributeNames);
        Assert.NotEmpty(check.ExpressionAttributeValues);

        // The attribute name token should resolve to "status"
        Assert.Contains(check.ExpressionAttributeNames.Values, v => v == "status");
        // The attribute value should be "Active"
        Assert.Contains(check.ExpressionAttributeValues.Values, v => v.S == "Active");
    }

    // ──────────────────────────────────────────────
    //  Patch — composite key
    // ──────────────────────────────────────────────

    [Fact]
    public void PatchTransactionRequest_CompositeKey_SetsBothKeys()
    {
        var property = new Property { Name = "Status", Value = "Shipped" };
        var request = new PatchTransactionRequest<OrderTestEntity>("ord-1", "sk-1", property);

        Assert.Equal(2, request.Key.Count);
        Assert.Equal("ord-1", request.Key["order_id"].S);
        Assert.Equal("sk-1", request.Key["sort_key"].S);
    }

    [Fact]
    public void PatchTransactionRequest_CompositeKey_GetOperation_HasCorrectKey()
    {
        var property = new Property { Name = "Status", Value = "Shipped" };
        var request = new PatchTransactionRequest<OrderTestEntity>("ord-1", "sk-1", property);

        var operation = request.GetOperation();

        Assert.NotNull(operation.UpdateType);
        var update = operation.UpdateType!;
        Assert.Equal("Orders", update.TableName);
        Assert.Equal(2, update.Key.Count);
        Assert.Equal("ord-1", update.Key["order_id"].S);
        Assert.Equal("sk-1", update.Key["sort_key"].S);
        Assert.Equal("SET #Property = :newValue", update.UpdateExpression);
    }

    // ──────────────────────────────────────────────
    //  Negative: hash-only entity + range key
    // ──────────────────────────────────────────────

    [Fact]
    public void DynamoDbTransactor_DeleteAsync_CompositeKey_ThrowsForHashOnlyEntity()
    {
        // ProductTestEntity has no DynamoDBRangeKey — constructing a
        // DeleteTransactionRequest with two key args should throw because
        // GetRangeKeyAttributeName will fail.
        Assert.ThrowsAny<ArgumentException>(() =>
            new DeleteTransactionRequest<ProductTestEntity>("prod-1", "some-range"));
    }

    // ──────────────────────────────────────────────
    //  Transactor-level: composite key ConditionEquals
    // ──────────────────────────────────────────────

    [Fact]
    public async Task DynamoDbTransactor_ConditionEquals_CompositeKey_BuildsRequest()
    {
        var mockManager = new Mock<ITransactionManager>();
        IEnumerable<ITransactionRequest>? captured = null;

        mockManager.Setup(m => m.ExecuteTransactionAsync(
                It.IsAny<IEnumerable<ITransactionRequest>>(),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<ITransactionRequest>, TransactionOptions?, CancellationToken>(
                (reqs, _, _) => captured = reqs.ToList())
            .ReturnsAsync((TransactWriteItemsResponse?)null);

        await using (var transactor = new DynamoDbTransactor(mockManager.Object))
        {
            transactor.ConditionEquals<OrderTestEntity, string>(
                "ord-1", "sk-1", x => x.Status, "Active");
        }

        Assert.NotNull(captured);
        var list = captured!.ToList();
        Assert.Single(list);

        var req = list[0];
        Assert.Equal(2, req.Key.Count);
        Assert.Equal("ord-1", req.Key["order_id"].S);
        Assert.Equal("sk-1", req.Key["sort_key"].S);
    }
}
