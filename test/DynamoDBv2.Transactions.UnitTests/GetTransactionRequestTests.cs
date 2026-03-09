using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

[DynamoDBTable("Orders")]
public partial class OrderTestEntity
{
    [DynamoDBHashKey(AttributeName = "order_id")]
    public string OrderId { get; set; } = "";

    [DynamoDBRangeKey(AttributeName = "sort_key")]
    public string SortKey { get; set; } = "";

    [DynamoDBProperty(AttributeName = "status")]
    public string Status { get; set; } = "";

    [DynamoDBProperty(AttributeName = "total")]
    public double Total { get; set; }

    [DynamoDBProperty(AttributeName = "customer_name")]
    public string CustomerName { get; set; } = "";

    [DynamoDBVersion]
    public long? Version { get; set; }
}

[DynamoDBTable("Products")]
public partial class ProductTestEntity
{
    [DynamoDBHashKey(AttributeName = "product_id")]
    public string ProductId { get; set; } = "";

    public string Name { get; set; } = "";

    public decimal Price { get; set; }

    public bool InStock { get; set; }
}

public class GetTransactionRequestTests
{
    [Fact]
    public void Constructor_HashKeyOnly_SetsTableAndKey()
    {
        var request = new GetTransactionRequest<OrderTestEntity>("order-123");

        Assert.Equal("Orders", request.TableName);
        Assert.Single(request.Key);
        Assert.Equal("order-123", request.Key["order_id"].S);
        Assert.Null(request.ProjectionExpression);
        Assert.Empty(request.ExpressionAttributeNames);
        Assert.Equal(typeof(OrderTestEntity), request.ItemType);
    }

    [Fact]
    public void Constructor_CompositeKey_SetsBothKeys()
    {
        var request = new GetTransactionRequest<OrderTestEntity>("order-123", "SK#2024");

        Assert.Equal(2, request.Key.Count);
        Assert.Equal("order-123", request.Key["order_id"].S);
        Assert.Equal("SK#2024", request.Key["sort_key"].S);
    }

    [Fact]
    public void Constructor_WithProjection_SetsProjectionExpression()
    {
        var request = new GetTransactionRequest<OrderTestEntity>(
            "order-123",
            x => new { x.Status, x.Total });

        Assert.NotNull(request.ProjectionExpression);
        Assert.Contains("#proj0", request.ProjectionExpression);
        Assert.Contains("#proj1", request.ProjectionExpression);
        Assert.Equal("status", request.ExpressionAttributeNames["#proj0"]);
        Assert.Equal("total", request.ExpressionAttributeNames["#proj1"]);
    }

    [Fact]
    public void Constructor_WithProjection_CompositeKey_SetsBothKeysAndProjection()
    {
        var request = new GetTransactionRequest<OrderTestEntity>(
            "order-123",
            "SK#2024",
            x => new { x.CustomerName });

        Assert.Equal(2, request.Key.Count);
        Assert.NotNull(request.ProjectionExpression);
        Assert.Equal("customer_name", request.ExpressionAttributeNames["#proj0"]);
    }

    [Fact]
    public void Constructor_SinglePropertyProjection_Works()
    {
        var request = new GetTransactionRequest<ProductTestEntity>(
            "prod-1",
            x => x.Name);

        Assert.NotNull(request.ProjectionExpression);
        Assert.Single(request.ExpressionAttributeNames);
        Assert.Equal("Name", request.ExpressionAttributeNames["#proj0"]);
    }

    [Fact]
    public void Constructor_NullHashKey_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new GetTransactionRequest<OrderTestEntity>((string)null!));
    }

    [Fact]
    public void Constructor_NullProjection_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new GetTransactionRequest<OrderTestEntity>("key", (System.Linq.Expressions.Expression<Func<OrderTestEntity, object>>)null!));
    }

    // ──────────────────────────────────────────────
    //  New tests
    // ──────────────────────────────────────────────

    [Fact]
    public void Constructor_CompositeKey_NullRangeKey_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new GetTransactionRequest<OrderTestEntity>("order-1", (string)null!));
    }

    [Fact]
    public void Constructor_MultipleProjectionProperties_SetsAll()
    {
        var request = new GetTransactionRequest<OrderTestEntity>(
            "order-1",
            x => new { x.OrderId, x.Status, x.Total, x.CustomerName });

        Assert.NotNull(request.ProjectionExpression);
        Assert.Equal(4, request.ExpressionAttributeNames.Count);
        Assert.Equal("order_id", request.ExpressionAttributeNames["#proj0"]);
        Assert.Equal("status", request.ExpressionAttributeNames["#proj1"]);
        Assert.Equal("total", request.ExpressionAttributeNames["#proj2"]);
        Assert.Equal("customer_name", request.ExpressionAttributeNames["#proj3"]);
        Assert.Equal("#proj0, #proj1, #proj2, #proj3", request.ProjectionExpression);
    }

    [Fact]
    public void Constructor_ValueTypeProjection_Works()
    {
        // bool and decimal are value types; the projection lambda wraps them in Convert
        var request = new GetTransactionRequest<ProductTestEntity>(
            "prod-1",
            x => x.InStock);

        Assert.NotNull(request.ProjectionExpression);
        Assert.Single(request.ExpressionAttributeNames);
        Assert.Equal("InStock", request.ExpressionAttributeNames["#proj0"]);
    }

    [Fact]
    public void Constructor_DecimalProjection_Works()
    {
        var request = new GetTransactionRequest<ProductTestEntity>(
            "prod-1",
            x => x.Price);

        Assert.NotNull(request.ProjectionExpression);
        Assert.Single(request.ExpressionAttributeNames);
        Assert.Equal("Price", request.ExpressionAttributeNames["#proj0"]);
    }

    [Fact]
    public void ItemType_ReturnsCorrectType_Product()
    {
        var request = new GetTransactionRequest<ProductTestEntity>("prod-1");
        Assert.Equal(typeof(ProductTestEntity), request.ItemType);
    }

    [Fact]
    public void ItemType_ReturnsCorrectType_Order()
    {
        var request = new GetTransactionRequest<OrderTestEntity>("ord-1");
        Assert.Equal(typeof(OrderTestEntity), request.ItemType);
    }

    [Fact]
    public void TableName_FromDynamoDBTableAttribute()
    {
        var request = new GetTransactionRequest<ProductTestEntity>("prod-1");
        Assert.Equal("Products", request.TableName);
    }

    [Fact]
    public void HashKeyAttributeName_UsesAttributeNameProperty()
    {
        var request = new GetTransactionRequest<OrderTestEntity>("ord-1");
        Assert.True(request.Key.ContainsKey("order_id"));
    }

    [Fact]
    public void RangeKeyAttributeName_UsesAttributeNameProperty()
    {
        var request = new GetTransactionRequest<OrderTestEntity>("ord-1", "sk-1");
        Assert.True(request.Key.ContainsKey("sort_key"));
    }

    [Fact]
    public void Constructor_CompositeKeyWithProjectionAndNullProjection_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new GetTransactionRequest<OrderTestEntity>(
                "ord-1",
                "sk-1",
                (System.Linq.Expressions.Expression<Func<OrderTestEntity, object>>)null!));
    }

    [Fact]
    public void Constructor_HashKeyOnly_NoRangeKey_SingleKeyEntry()
    {
        // ProductTestEntity has no range key; verify only one key in dict
        var request = new GetTransactionRequest<ProductTestEntity>("prod-1");
        Assert.Single(request.Key);
        Assert.True(request.Key.ContainsKey("product_id"));
    }

    [Fact]
    public void Constructor_SimpleHashKeyEntity_UsesPropertyNameAsKeyName()
    {
        // SimpleHashKeyEntity has [DynamoDBHashKey] without AttributeName
        var request = new GetTransactionRequest<SimpleHashKeyEntity>("id-1");
        Assert.True(request.Key.ContainsKey("Id"));
        Assert.Equal("id-1", request.Key["Id"].S);
    }
}
