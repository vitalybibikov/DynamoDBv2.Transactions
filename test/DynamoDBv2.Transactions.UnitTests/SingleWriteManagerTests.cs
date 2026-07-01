using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for <see cref="SingleWriteManager"/> — maps a single request's <c>GetOperation()</c> onto the
/// standalone <c>UpdateItem</c>/<c>PutItem</c>/<c>DeleteItem</c> API (no transaction). Uses a mocked
/// <see cref="IAmazonDynamoDB"/> and captures the request the manager dispatches.
/// </summary>
public class SingleWriteManagerTests
{
    private readonly Mock<IAmazonDynamoDB> _client = new();

    private static OrderTestEntity CreateOrder() => new()
    {
        OrderId = "order-1",
        SortKey = "sort-1",
        Status = "Closed",
        Total = 42.5,
        CustomerName = "Alice",
        Version = 7
    };

    [Fact]
    public async Task PatchMany_DispatchesUpdateItem_WithScopedExpressionAndAddVersion()
    {
        UpdateItemRequest? captured = null;
        _client.Setup(c => c.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback<UpdateItemRequest, CancellationToken>((r, _) => captured = r)
            .ReturnsAsync(new UpdateItemResponse());

        var manager = new SingleWriteManager(_client.Object);
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(), new[] { "Status", "Total" }, incrementVersion: true);

        await manager.ExecuteAsync(request);

        Assert.NotNull(captured);
        Assert.Equal(request.TableName, captured!.TableName);
        Assert.Equal(request.UpdateExpression, captured.UpdateExpression);
        Assert.StartsWith("SET ", captured.UpdateExpression);
        Assert.Contains("ADD #version :increment", captured.UpdateExpression);
        Assert.Equal("attribute_exists(#hashKey)", captured.ConditionExpression);
        Assert.Equal(2, captured.Key.Count);
        Assert.Equal("order-1", captured.Key["order_id"].S);
        Assert.Equal("sort-1", captured.Key["sort_key"].S);
        Assert.True(captured.ExpressionAttributeNames.Count > 0);
        Assert.True(captured.ExpressionAttributeValues.Count > 0);
        _client.Verify(c => c.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        _client.Verify(c => c.TransactWriteItemsAsync(It.IsAny<TransactWriteItemsRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Patch_SingleAttribute_DispatchesUpdateItem()
    {
        UpdateItemRequest? captured = null;
        _client.Setup(c => c.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback<UpdateItemRequest, CancellationToken>((r, _) => captured = r)
            .ReturnsAsync(new UpdateItemResponse());

        var manager = new SingleWriteManager(_client.Object);
        var request = new PatchTransactionRequest<OrderTestEntity>(CreateOrder(), "Status");

        await manager.ExecuteAsync(request);

        Assert.NotNull(captured);
        Assert.Equal(request.UpdateExpression, captured!.UpdateExpression);
    }

    [Fact]
    public async Task Put_DispatchesPutItem_WithItem()
    {
        PutItemRequest? captured = null;
        _client.Setup(c => c.PutItemAsync(It.IsAny<PutItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback<PutItemRequest, CancellationToken>((r, _) => captured = r)
            .ReturnsAsync(new PutItemResponse());

        var manager = new SingleWriteManager(_client.Object);
        var request = new PutTransactionRequest<OrderTestEntity>(CreateOrder());

        await manager.ExecuteAsync(request);

        Assert.NotNull(captured);
        Assert.Equal(request.TableName, captured!.TableName);
        Assert.True(captured.Item.Count > 0);
        Assert.Equal("order-1", captured.Item["order_id"].S);
    }

    [Fact]
    public async Task Put_NonVersionedEntity_HasNoConditionOrExpressionAttributes()
    {
        PutItemRequest? captured = null;
        _client.Setup(c => c.PutItemAsync(It.IsAny<PutItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback<PutItemRequest, CancellationToken>((r, _) => captured = r)
            .ReturnsAsync(new PutItemResponse());

        var manager = new SingleWriteManager(_client.Object);
        var product = new ProductTestEntity { ProductId = "prod-1", Name = "Widget", Price = 1m, InStock = true };

        await manager.ExecuteAsync(new PutTransactionRequest<ProductTestEntity>(product));

        Assert.NotNull(captured);
        Assert.Null(captured!.ConditionExpression);
        Assert.True(captured.Item.Count > 0);
    }

    [Fact]
    public async Task Delete_WithConditionAndExpressionAttributesAndReturnValues_AllCopiedThrough()
    {
        DeleteItemRequest? captured = null;
        _client.Setup(c => c.DeleteItemAsync(It.IsAny<DeleteItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback<DeleteItemRequest, CancellationToken>((r, _) => captured = r)
            .ReturnsAsync(new DeleteItemResponse());

        var manager = new SingleWriteManager(_client.Object);
        var request = new DeleteTransactionRequest<ProductTestEntity>("prod-1")
        {
            ConditionExpression = "attribute_exists(#pk)",
            ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
        };
        request.ExpressionAttributeNames["#pk"] = "product_id";
        request.ExpressionAttributeValues[":v"] = new AttributeValue { S = "prod-1" };

        await manager.ExecuteAsync(request);

        Assert.NotNull(captured);
        Assert.Equal("attribute_exists(#pk)", captured!.ConditionExpression);
        Assert.Equal("product_id", captured.ExpressionAttributeNames["#pk"]);
        Assert.Equal("prod-1", captured.ExpressionAttributeValues[":v"].S);
        Assert.Equal(ReturnValuesOnConditionCheckFailure.ALL_OLD, captured.ReturnValuesOnConditionCheckFailure);
    }

    [Fact]
    public async Task Delete_DispatchesDeleteItem_WithKey()
    {
        DeleteItemRequest? captured = null;
        _client.Setup(c => c.DeleteItemAsync(It.IsAny<DeleteItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback<DeleteItemRequest, CancellationToken>((r, _) => captured = r)
            .ReturnsAsync(new DeleteItemResponse());

        var manager = new SingleWriteManager(_client.Object);
        var request = new DeleteTransactionRequest<ProductTestEntity>("prod-1");

        await manager.ExecuteAsync(request);

        Assert.NotNull(captured);
        Assert.Equal("prod-1", captured!.Key["product_id"].S);
    }

    [Fact]
    public async Task ReturnValuesOnConditionCheckFailure_IsCopiedThrough()
    {
        UpdateItemRequest? captured = null;
        _client.Setup(c => c.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback<UpdateItemRequest, CancellationToken>((r, _) => captured = r)
            .ReturnsAsync(new UpdateItemResponse());

        var manager = new SingleWriteManager(_client.Object);
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(), new[] { "Status" }, incrementVersion: false)
        {
            ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.ALL_OLD
        };

        await manager.ExecuteAsync(request);

        Assert.NotNull(captured);
        Assert.Equal(ReturnValuesOnConditionCheckFailure.ALL_OLD, captured!.ReturnValuesOnConditionCheckFailure);
    }

    [Fact]
    public async Task NullRequest_Throws()
    {
        var manager = new SingleWriteManager(_client.Object);
        await Assert.ThrowsAsync<ArgumentNullException>(() => manager.ExecuteAsync(null!));
    }

    [Fact]
    public async Task NonTransactionRequest_Throws()
    {
        var manager = new SingleWriteManager(_client.Object);
        await Assert.ThrowsAsync<ArgumentException>(() => manager.ExecuteAsync(new FakeRequest()));
    }

    [Fact]
    public async Task ConditionCheckRequest_Throws()
    {
        var manager = new SingleWriteManager(_client.Object);
        var request = new ConditionCheckTransactionRequest<OrderTestEntity>("order-1", "sort-1");
        request.Equals<OrderTestEntity, string>(o => o.Status, "Closed");

        await Assert.ThrowsAsync<ArgumentException>(() => manager.ExecuteAsync(request));
    }

    private sealed class FakeRequest : ITransactionRequest
    {
        public string TableName => "Fake";
        public System.Collections.Generic.Dictionary<string, AttributeValue> Key { get; } = new();
        public string? ConditionExpression => null;
        public TransactOperationType Type => TransactOperationType.Put;
        public System.Collections.Generic.Dictionary<string, string> ExpressionAttributeNames { get; set; } = new();
        public System.Collections.Generic.Dictionary<string, AttributeValue> ExpressionAttributeValues { get; set; } = new();
    }
}
