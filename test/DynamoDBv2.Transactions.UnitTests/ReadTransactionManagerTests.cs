using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

public class ReadTransactionManagerTests
{
    [Fact]
    public async Task ExecuteGetTransactionAsync_BuildsCorrectRequest()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>
                    {
                        { "product_id", new AttributeValue { S = "prod-1" } }
                    }}
                }
            });

        var manager = new ReadTransactionManager(mockClient.Object);
        var request = new GetTransactionRequest<ProductTestEntity>("prod-1");

        await manager.ExecuteGetTransactionAsync(new List<IGetTransactionRequest> { request });

        Assert.NotNull(capturedRequest);
        Assert.Single(capturedRequest!.TransactItems);
        Assert.Equal("Products", capturedRequest.TransactItems[0].Get.TableName);
        Assert.Equal("prod-1", capturedRequest.TransactItems[0].Get.Key["product_id"].S);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_WithProjection_SetsProjectionExpression()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>()
            });

        var manager = new ReadTransactionManager(mockClient.Object);
        var request = new GetTransactionRequest<ProductTestEntity>(
            "prod-1",
            x => new { x.Name, x.Price });

        await manager.ExecuteGetTransactionAsync(new List<IGetTransactionRequest> { request });

        Assert.NotNull(capturedRequest);
        var get = capturedRequest!.TransactItems[0].Get;
        Assert.NotNull(get.ProjectionExpression);
        Assert.Equal(2, get.ExpressionAttributeNames.Count);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_WithOptions_SetsConsumedCapacity()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse());

        var manager = new ReadTransactionManager(mockClient.Object);
        var options = new ReadTransactionOptions
        {
            ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
        };

        await manager.ExecuteGetTransactionAsync(
            new List<IGetTransactionRequest>
            {
                new GetTransactionRequest<ProductTestEntity>("prod-1")
            },
            options);

        Assert.NotNull(capturedRequest);
        Assert.Equal(ReturnConsumedCapacity.TOTAL, capturedRequest!.ReturnConsumedCapacity);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_Over100Items_ThrowsArgumentException()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new ReadTransactionManager(mockClient.Object);

        var requests = Enumerable.Range(0, 101)
            .Select(i => new GetTransactionRequest<ProductTestEntity>($"prod-{i}"))
            .Cast<IGetTransactionRequest>()
            .ToList();

        await Assert.ThrowsAsync<ArgumentException>(
            () => manager.ExecuteGetTransactionAsync(requests));
    }

    // ──────────────────────────────────────────────
    //  New tests
    // ──────────────────────────────────────────────

    [Fact]
    public async Task ExecuteGetTransactionAsync_EmptyRequestList_ThrowsArgumentException()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new ReadTransactionManager(mockClient.Object);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            manager.ExecuteGetTransactionAsync(new List<IGetTransactionRequest>()));

        Assert.Contains("at least one item", ex.Message);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_Exactly100Items_Succeeds()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = Enumerable.Range(0, 100)
                    .Select(_ => new ItemResponse { Item = new Dictionary<string, AttributeValue>() })
                    .ToList()
            });

        var manager = new ReadTransactionManager(mockClient.Object);
        var requests = Enumerable.Range(0, 100)
            .Select(i => new GetTransactionRequest<ProductTestEntity>($"prod-{i}"))
            .Cast<IGetTransactionRequest>()
            .ToList();

        var result = await manager.ExecuteGetTransactionAsync(requests);

        Assert.NotNull(capturedRequest);
        Assert.Equal(100, capturedRequest!.TransactItems.Count);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_MultipleItemsDifferentTables()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>() },
                    new() { Item = new Dictionary<string, AttributeValue>() },
                    new() { Item = new Dictionary<string, AttributeValue>() }
                }
            });

        var manager = new ReadTransactionManager(mockClient.Object);
        var requests = new List<IGetTransactionRequest>
        {
            new GetTransactionRequest<ProductTestEntity>("prod-1"),
            new GetTransactionRequest<OrderTestEntity>("ord-1", "sk-1"),
            new GetTransactionRequest<SomeDynamoDbEntity>("some-1")
        };

        await manager.ExecuteGetTransactionAsync(requests);

        Assert.NotNull(capturedRequest);
        Assert.Equal(3, capturedRequest!.TransactItems.Count);
        Assert.Equal("Products", capturedRequest.TransactItems[0].Get.TableName);
        Assert.Equal("Orders", capturedRequest.TransactItems[1].Get.TableName);
        Assert.Equal("SomeDynamoDbEntity", capturedRequest.TransactItems[2].Get.TableName);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_NoProjection_ProjectionExpressionNotSet()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>() }
                }
            });

        var manager = new ReadTransactionManager(mockClient.Object);
        var request = new GetTransactionRequest<ProductTestEntity>("prod-1");

        await manager.ExecuteGetTransactionAsync(new List<IGetTransactionRequest> { request });

        Assert.NotNull(capturedRequest);
        var get = capturedRequest!.TransactItems[0].Get;
        Assert.Null(get.ProjectionExpression);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_CancellationToken_IsPassedThrough()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        CancellationToken capturedToken = default;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((_, ct) => capturedToken = ct)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>()
            });

        var manager = new ReadTransactionManager(mockClient.Object);
        using var cts = new CancellationTokenSource();
        var expectedToken = cts.Token;

        await manager.ExecuteGetTransactionAsync(
            new List<IGetTransactionRequest>
            {
                new GetTransactionRequest<ProductTestEntity>("prod-1")
            },
            null,
            expectedToken);

        Assert.Equal(expectedToken, capturedToken);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_WithoutOptions_DoesNotSetConsumedCapacity()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse());

        var manager = new ReadTransactionManager(mockClient.Object);

        await manager.ExecuteGetTransactionAsync(
            new List<IGetTransactionRequest>
            {
                new GetTransactionRequest<ProductTestEntity>("prod-1")
            });

        Assert.NotNull(capturedRequest);
        // ReturnConsumedCapacity should be null/default (NONE)
        Assert.Null(capturedRequest!.ReturnConsumedCapacity);
    }

    [Fact]
    public async Task ExecuteGetTransactionAsync_CompositeKeyRequest_SetsBothKeysOnGet()
    {
        var mockClient = new Mock<IAmazonDynamoDB>();
        TransactGetItemsRequest? capturedRequest = null;

        mockClient.Setup(c => c.TransactGetItemsAsync(
            It.IsAny<TransactGetItemsRequest>(),
            It.IsAny<CancellationToken>()))
            .Callback<TransactGetItemsRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>() }
                }
            });

        var manager = new ReadTransactionManager(mockClient.Object);
        var request = new GetTransactionRequest<OrderTestEntity>("ord-1", "sk-1");

        await manager.ExecuteGetTransactionAsync(new List<IGetTransactionRequest> { request });

        Assert.NotNull(capturedRequest);
        var get = capturedRequest!.TransactItems[0].Get;
        Assert.Equal(2, get.Key.Count);
        Assert.Equal("ord-1", get.Key["order_id"].S);
        Assert.Equal("sk-1", get.Key["sort_key"].S);
    }
}
