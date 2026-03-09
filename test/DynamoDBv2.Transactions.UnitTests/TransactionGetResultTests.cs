using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

public class TransactionGetResultTests
{
    private static async Task<TransactionGetResult> CreateResultWithItems(
        params (Type type, Dictionary<string, AttributeValue>? attrs)[] items)
    {
        return await CreateResultWithItemsAndCapacity(null, items);
    }

    private static async Task<TransactionGetResult> CreateResultWithItemsAndCapacity(
        List<ConsumedCapacity>? consumedCapacity,
        params (Type type, Dictionary<string, AttributeValue>? attrs)[] items)
    {
        var responses = items.Select(i => new ItemResponse
        {
            Item = i.attrs ?? new Dictionary<string, AttributeValue>()
        }).ToList();

        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactGetItemsResponse
            {
                Responses = responses,
                ConsumedCapacity = consumedCapacity
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);

        foreach (var item in items)
        {
            if (item.type == typeof(ProductTestEntity))
            {
                transactor.Get<ProductTestEntity>("dummy");
            }
            else if (item.type == typeof(OrderTestEntity))
            {
                transactor.Get<OrderTestEntity>("dummy", "dummy");
            }
            else if (item.type == typeof(SomeDynamoDbEntity))
            {
                transactor.Get<SomeDynamoDbEntity>("dummy");
            }
        }

        return await transactor.ExecuteAsync();
    }

    [Fact]
    public async Task GetItem_ValidIndex_DeserializesEntity()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "product_id", new AttributeValue { S = "prod-1" } },
            { "Name", new AttributeValue { S = "Widget" } },
            { "Price", new AttributeValue { N = "19.99" } },
            { "InStock", new AttributeValue { BOOL = true } }
        };

        var result = await CreateResultWithItems((typeof(ProductTestEntity), attrs));

        var product = result.GetItem<ProductTestEntity>(0);
        Assert.NotNull(product);
        Assert.Equal("prod-1", product!.ProductId);
        Assert.Equal("Widget", product.Name);
        Assert.Equal(19.99m, product.Price);
        Assert.True(product.InStock);
    }

    [Fact]
    public async Task GetItem_EmptyAttributes_ReturnsNull()
    {
        var result = await CreateResultWithItems((typeof(ProductTestEntity), new Dictionary<string, AttributeValue>()));
        Assert.Null(result.GetItem<ProductTestEntity>(0));
    }

    [Fact]
    public async Task GetItem_OutOfRange_ThrowsArgumentOutOfRangeException()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactGetItemsResponse { Responses = new List<ItemResponse>() });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        var result = await transactor.ExecuteAsync();
        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetItem<ProductTestEntity>(0));
    }

    [Fact]
    public async Task GetRawItem_ReturnsAttributes()
    {
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "product_id", new AttributeValue { S = "prod-1" } }
        };

        var result = await CreateResultWithItems((typeof(ProductTestEntity), attrs));
        var raw = result.GetRawItem(0);
        Assert.NotNull(raw);
        Assert.Equal("prod-1", raw!["product_id"].S);
    }

    [Fact]
    public async Task Count_ReturnsItemCount()
    {
        var result = await CreateResultWithItems(
            (typeof(ProductTestEntity), null),
            (typeof(OrderTestEntity), null));

        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task GetItems_FiltersByType()
    {
        var productAttrs = new Dictionary<string, AttributeValue>
        {
            { "product_id", new AttributeValue { S = "prod-1" } },
            { "Name", new AttributeValue { S = "W" } },
            { "Price", new AttributeValue { N = "10" } },
            { "InStock", new AttributeValue { BOOL = true } }
        };

        var orderAttrs = new Dictionary<string, AttributeValue>
        {
            { "order_id", new AttributeValue { S = "ord-1" } },
            { "sort_key", new AttributeValue { S = "sk" } },
            { "status", new AttributeValue { S = "shipped" } },
            { "total", new AttributeValue { N = "50" } },
            { "customer_name", new AttributeValue { S = "Test" } }
        };

        var result = await CreateResultWithItems(
            (typeof(ProductTestEntity), productAttrs),
            (typeof(OrderTestEntity), orderAttrs));

        var products = result.GetItems<ProductTestEntity>();
        Assert.Single(products);
        Assert.Equal("prod-1", products[0].ProductId);
    }

    // ──────────────────────────────────────────────
    //  New tests
    // ──────────────────────────────────────────────

    [Fact]
    public async Task GetRawItem_OutOfRange_ThrowsArgumentOutOfRangeException()
    {
        var mockManager = new Mock<IReadTransactionManager>();
        mockManager.Setup(m => m.ExecuteGetTransactionAsync(
            It.IsAny<IEnumerable<IGetTransactionRequest>>(),
            It.IsAny<ReadTransactionOptions?>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactGetItemsResponse { Responses = new List<ItemResponse>() });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        var result = await transactor.ExecuteAsync();

        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetRawItem(0));
    }

    [Fact]
    public async Task GetRawItem_NegativeIndex_ThrowsArgumentOutOfRangeException()
    {
        var result = await CreateResultWithItems(
            (typeof(ProductTestEntity), new Dictionary<string, AttributeValue>
            {
                { "product_id", new AttributeValue { S = "p1" } }
            }));

        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetRawItem(-1));
    }

    [Fact]
    public async Task GetItem_NegativeIndex_ThrowsArgumentOutOfRangeException()
    {
        var result = await CreateResultWithItems(
            (typeof(ProductTestEntity), new Dictionary<string, AttributeValue>
            {
                { "product_id", new AttributeValue { S = "p1" } }
            }));

        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetItem<ProductTestEntity>(-1));
    }

    [Fact]
    public async Task GetItem_WrongType_StillDeserializesFromRawDict()
    {
        // GetItem<T> deserializes using DynamoDbMapper.MapFromAttributes(typeof(T), attrs)
        // even if the original request was for a different type. It just uses whatever attrs are there.
        var attrs = new Dictionary<string, AttributeValue>
        {
            { "product_id", new AttributeValue { S = "prod-1" } },
            { "Name", new AttributeValue { S = "Widget" } },
            { "Price", new AttributeValue { N = "10" } },
            { "InStock", new AttributeValue { BOOL = true } }
        };

        var result = await CreateResultWithItems((typeof(ProductTestEntity), attrs));

        // Deserialize as OrderTestEntity — mismatched type, but MapFromAttributes still works
        // (unmatched fields get defaults)
        var order = result.GetItem<OrderTestEntity>(0);
        Assert.NotNull(order);
        // OrderTestEntity doesn't have matching attrs, so fields stay at defaults
        Assert.Equal("", order!.OrderId);
    }

    [Fact]
    public async Task GetItems_ReturnsEmptyForTypeNotInResults()
    {
        var productAttrs = new Dictionary<string, AttributeValue>
        {
            { "product_id", new AttributeValue { S = "prod-1" } },
            { "Name", new AttributeValue { S = "W" } },
            { "Price", new AttributeValue { N = "10" } },
            { "InStock", new AttributeValue { BOOL = true } }
        };

        var result = await CreateResultWithItems((typeof(ProductTestEntity), productAttrs));

        // Ask for OrderTestEntity which was never requested
        var orders = result.GetItems<OrderTestEntity>();
        Assert.Empty(orders);
    }

    [Fact]
    public async Task ConsumedCapacity_FlowsThrough()
    {
        var capacity = new List<ConsumedCapacity>
        {
            new ConsumedCapacity
            {
                TableName = "Products",
                CapacityUnits = 5.0
            }
        };

        var result = await CreateResultWithItemsAndCapacity(
            capacity,
            (typeof(ProductTestEntity), new Dictionary<string, AttributeValue>
            {
                { "product_id", new AttributeValue { S = "p1" } },
                { "Name", new AttributeValue { S = "W" } },
                { "Price", new AttributeValue { N = "1" } },
                { "InStock", new AttributeValue { BOOL = true } }
            }));

        Assert.NotNull(result.ConsumedCapacity);
        Assert.Single(result.ConsumedCapacity!);
        Assert.Equal("Products", result.ConsumedCapacity[0].TableName);
        Assert.Equal(5.0, result.ConsumedCapacity[0].CapacityUnits);
    }

    [Fact]
    public async Task ConsumedCapacity_IsNull_WhenNotRequested()
    {
        var result = await CreateResultWithItemsAndCapacity(
            null,
            (typeof(ProductTestEntity), new Dictionary<string, AttributeValue>
            {
                { "product_id", new AttributeValue { S = "p1" } }
            }));

        Assert.Null(result.ConsumedCapacity);
    }

    [Fact]
    public async Task GetItems_MultipleItemsSameType()
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
                        { "product_id", new AttributeValue { S = "prod-2" } },
                        { "Name", new AttributeValue { S = "Gadget" } },
                        { "Price", new AttributeValue { N = "20" } },
                        { "InStock", new AttributeValue { BOOL = false } }
                    }},
                    new() { Item = new Dictionary<string, AttributeValue>
                    {
                        { "product_id", new AttributeValue { S = "prod-3" } },
                        { "Name", new AttributeValue { S = "Doohickey" } },
                        { "Price", new AttributeValue { N = "30" } },
                        { "InStock", new AttributeValue { BOOL = true } }
                    }}
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1");
        transactor.Get<ProductTestEntity>("prod-2");
        transactor.Get<ProductTestEntity>("prod-3");

        var result = await transactor.ExecuteAsync();

        var products = result.GetItems<ProductTestEntity>();
        Assert.Equal(3, products.Count);
        Assert.Equal("prod-1", products[0].ProductId);
        Assert.Equal("prod-2", products[1].ProductId);
        Assert.Equal("prod-3", products[2].ProductId);
    }

    [Fact]
    public async Task Count_ZeroItems()
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
    public async Task GetItems_SkipsEmptyAttributeItems()
    {
        // GetItems only includes items with non-empty attributes
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
                    new() { Item = new Dictionary<string, AttributeValue>() } // empty = not found
                }
            });

        var transactor = new DynamoDbReadTransactor(mockManager.Object);
        transactor.Get<ProductTestEntity>("prod-1");
        transactor.Get<ProductTestEntity>("prod-missing");

        var result = await transactor.ExecuteAsync();

        Assert.Equal(2, result.Count); // Count includes all items
        var products = result.GetItems<ProductTestEntity>();
        Assert.Single(products); // GetItems skips empty-attribute items
        Assert.Equal("prod-1", products[0].ProductId);
    }
}
