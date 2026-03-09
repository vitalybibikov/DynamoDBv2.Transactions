using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

public class DynamoDbReadTransactorTests
{
    [Fact]
    public async Task ExecuteAsync_SingleGet_ReturnsTypedResult()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new()
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { "product_id", new AttributeValue { S = "prod-1" } },
                            { "Name", new AttributeValue { S = "Widget" } },
                            { "Price", new AttributeValue { N = "29.99" } },
                            { "InStock", new AttributeValue { BOOL = true } }
                        }
                    }
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1");

        var result = await transactor.ExecuteAsync();

        Assert.Equal(1, result.Count);
        var product = result.GetItem<ProductTestEntity>(0);
        Assert.NotNull(product);
        Assert.Equal("prod-1", product!.ProductId);
        Assert.Equal("Widget", product.Name);
        Assert.Equal(29.99m, product.Price);
        Assert.True(product.InStock);
    }

    [Fact]
    public async Task ExecuteAsync_MultipleTypes_ReturnsCorrectItems()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new()
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { "product_id", new AttributeValue { S = "prod-1" } },
                            { "Name", new AttributeValue { S = "Widget" } },
                            { "Price", new AttributeValue { N = "10" } },
                            { "InStock", new AttributeValue { BOOL = false } }
                        }
                    },
                    new()
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            { "order_id", new AttributeValue { S = "ord-1" } },
                            { "sort_key", new AttributeValue { S = "sk-1" } },
                            { "status", new AttributeValue { S = "shipped" } },
                            { "total", new AttributeValue { N = "100.50" } },
                            { "customer_name", new AttributeValue { S = "Alice" } }
                        }
                    }
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1");
        transactor.Get<OrderTestEntity>("ord-1", "sk-1");

        var result = await transactor.ExecuteAsync();

        Assert.Equal(2, result.Count);

        var product = result.GetItem<ProductTestEntity>(0);
        Assert.Equal("prod-1", product!.ProductId);

        var order = result.GetItem<OrderTestEntity>(1);
        Assert.Equal("ord-1", order!.OrderId);
        Assert.Equal("sk-1", order.SortKey);
        Assert.Equal("shipped", order.Status);
        Assert.Equal(100.50, order.Total);
        Assert.Equal("Alice", order.CustomerName);
    }

    [Fact]
    public async Task ExecuteAsync_WithProjection_PassesProjectionToManager()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        IEnumerable<IGetTransactionRequest>? capturedRequests = null;

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<IGetTransactionRequest>, ReadTransactionOptions?, CancellationToken>(
                (reqs, _, _) => capturedRequests = reqs.ToList())
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>() }
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1", x => new { x.Name, x.Price });

        await transactor.ExecuteAsync();

        Assert.NotNull(capturedRequests);
        var requests = capturedRequests!.ToList();
        Assert.Single(requests);
        Assert.NotNull(requests[0].ProjectionExpression);
    }

    [Fact]
    public async Task ExecuteAsync_NullResponse_ReturnsNullItems()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync((TransactGetItemsResponse?)null);

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1");

        var result = await transactor.ExecuteAsync();
        Assert.Equal(1, result.Count);
        Assert.Null(result.GetItem<ProductTestEntity>(0));
    }

    [Fact]
    public async Task ExecuteAsync_SetsOptions()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        ReadTransactionOptions? capturedOptions = null;

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<IGetTransactionRequest>, ReadTransactionOptions?, CancellationToken>(
                (_, opts, _) => capturedOptions = opts)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>()
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Options = new ReadTransactionOptions
        {
            ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
        };

        await transactor.ExecuteAsync();

        Assert.NotNull(capturedOptions);
        Assert.Equal(ReturnConsumedCapacity.TOTAL, capturedOptions!.ReturnConsumedCapacity);
    }

    // ──────────────────────────────────────────────
    //  New tests
    // ──────────────────────────────────────────────

    [Fact]
    public void Constructor_NullManager_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new DynamoDbReadTransactor((IReadTransactionManager)null!));
    }

    [Fact]
    public async Task Get_CompositeKey_FlowsThrough()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        IEnumerable<IGetTransactionRequest>? capturedRequests = null;

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<IGetTransactionRequest>, ReadTransactionOptions?, CancellationToken>(
                (reqs, _, _) => capturedRequests = reqs.ToList())
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>
                    {
                        { "order_id", new AttributeValue { S = "ord-1" } },
                        { "sort_key", new AttributeValue { S = "sk-1" } }
                    }}
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<OrderTestEntity>("ord-1", "sk-1");

        await transactor.ExecuteAsync();

        var requests = capturedRequests!.ToList();
        Assert.Single(requests);
        Assert.Equal(2, requests[0].Key.Count);
        Assert.Equal("ord-1", requests[0].Key["order_id"].S);
        Assert.Equal("sk-1", requests[0].Key["sort_key"].S);
    }

    [Fact]
    public async Task ExecuteAsync_CalledMultipleTimes_RebuildsResult()
    {
        var callCount = 0;
        var mockManager = new Mock<IReadTransactionManager>();

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                callCount++;
                return new TransactGetItemsResponse
                {
                    Responses = new List<ItemResponse>
                    {
                        new() { Item = new Dictionary<string, AttributeValue>
                        {
                            { "product_id", new AttributeValue { S = $"prod-{callCount}" } },
                            { "Name", new AttributeValue { S = "Test" } },
                            { "Price", new AttributeValue { N = "1" } },
                            { "InStock", new AttributeValue { BOOL = true } }
                        }}
                    }
                };
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1");

        var result1 = await transactor.ExecuteAsync();
        var result2 = await transactor.ExecuteAsync();

        Assert.Equal(2, callCount);
        Assert.Equal("prod-1", result1.GetItem<ProductTestEntity>(0)!.ProductId);
        Assert.Equal("prod-2", result2.GetItem<ProductTestEntity>(0)!.ProductId);
    }

    [Fact]
    public async Task ExecuteAsync_ThreeDifferentEntityTypes()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>
                    {
                        { "product_id", new AttributeValue { S = "prod-1" } },
                        { "Name", new AttributeValue { S = "Widget" } },
                        { "Price", new AttributeValue { N = "10" } },
                        { "InStock", new AttributeValue { BOOL = true } }
                    }},
                    new() { Item = new Dictionary<string, AttributeValue>
                    {
                        { "order_id", new AttributeValue { S = "ord-1" } },
                        { "sort_key", new AttributeValue { S = "sk-1" } },
                        { "status", new AttributeValue { S = "pending" } },
                        { "total", new AttributeValue { N = "50" } },
                        { "customer_name", new AttributeValue { S = "Bob" } }
                    }},
                    new() { Item = new Dictionary<string, AttributeValue>
                    {
                        { "MyId", new AttributeValue { S = "some-1" } },
                        { "Name", new AttributeValue { S = "SomeEntity" } },
                        { "Status", new AttributeValue { S = "Active" } },
                        { "Amount", new AttributeValue { N = "99.9" } }
                    }}
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1");
        transactor.Get<OrderTestEntity>("ord-1", "sk-1");
        transactor.Get<SomeDynamoDbEntity>("some-1");

        var result = await transactor.ExecuteAsync();

        Assert.Equal(3, result.Count);

        var product = result.GetItem<ProductTestEntity>(0);
        Assert.Equal("prod-1", product!.ProductId);

        var order = result.GetItem<OrderTestEntity>(1);
        Assert.Equal("ord-1", order!.OrderId);

        var some = result.GetItem<SomeDynamoDbEntity>(2);
        Assert.Equal("some-1", some!.Id);
        Assert.Equal("SomeEntity", some.Name);
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        var transactor = new DynamoDbReadTransactor(mockManager.Object);

        // Should not throw on multiple dispose calls
        await transactor.DisposeAsync();
        await transactor.DisposeAsync();
        await transactor.DisposeAsync();
    }

    [Fact]
    public async Task Options_FlowsThroughToManager()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        ReadTransactionOptions? capturedOptions = null;

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<IGetTransactionRequest>, ReadTransactionOptions?, CancellationToken>(
                (_, opts, _) => capturedOptions = opts)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>() }
                }
            });

        var options = new ReadTransactionOptions
        {
            ReturnConsumedCapacity = ReturnConsumedCapacity.INDEXES
        };

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Options = options;
        transactor.Get<ProductTestEntity>("prod-1");

        await transactor.ExecuteAsync();

        Assert.NotNull(capturedOptions);
        Assert.Equal(ReturnConsumedCapacity.INDEXES, capturedOptions!.ReturnConsumedCapacity);
    }

    [Fact]
    public async Task ExecuteAsync_NoGetsQueued_ReturnsEmptyResult()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>()
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);

        var result = await transactor.ExecuteAsync();
        Assert.Equal(0, result.Count);
    }

    [Fact]
    public async Task Get_WithCompositeKeyAndProjection_FlowsThrough()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        IEnumerable<IGetTransactionRequest>? capturedRequests = null;

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<IGetTransactionRequest>, ReadTransactionOptions?, CancellationToken>(
                (reqs, _, _) => capturedRequests = reqs.ToList())
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>
                {
                    new() { Item = new Dictionary<string, AttributeValue>
                    {
                        { "status", new AttributeValue { S = "shipped" } }
                    }}
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<OrderTestEntity>("ord-1", "sk-1", x => new { x.Status });

        await transactor.ExecuteAsync();

        var requests = capturedRequests!.ToList();
        Assert.Single(requests);
        Assert.NotNull(requests[0].ProjectionExpression);
        Assert.Equal(2, requests[0].Key.Count);
    }

    [Fact]
    public async Task ExecuteAsync_CancellationToken_FlowsThrough()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        CancellationToken capturedToken = default;

        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<IGetTransactionRequest>, ReadTransactionOptions?, CancellationToken>(
                (_, _, ct) => capturedToken = ct)
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = new List<ItemResponse>()
            });

        using var cts = new CancellationTokenSource();
        var expectedToken = cts.Token;

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        await transactor.ExecuteAsync(expectedToken);

        Assert.Equal(expectedToken, capturedToken);
    }
}
