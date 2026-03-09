using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

public class ProjectionBuilderTests
{
    [Fact]
    public void Build_AnonymousType_ExtractsMultipleProperties()
    {
        var (expr, names) = ProjectionBuilder.Build<OrderTestEntity>(
            x => new { x.Status, x.Total, x.CustomerName });

        Assert.Equal("#proj0, #proj1, #proj2", expr);
        Assert.Equal(3, names.Count);
        Assert.Equal("status", names["#proj0"]);
        Assert.Equal("total", names["#proj1"]);
        Assert.Equal("customer_name", names["#proj2"]);
    }

    [Fact]
    public void Build_SingleProperty_ExtractsOne()
    {
        var (expr, names) = ProjectionBuilder.Build<OrderTestEntity>(
            x => x.OrderId);

        Assert.Equal("#proj0", expr);
        Assert.Single(names);
        Assert.Equal("order_id", names["#proj0"]);
    }

    [Fact]
    public void Build_UsesAttributeNames_NotPropertyNames()
    {
        var (_, names) = ProjectionBuilder.Build<OrderTestEntity>(
            x => new { x.CustomerName });

        Assert.Equal("customer_name", names["#proj0"]);
    }

    [Fact]
    public void Build_PropertyWithoutAttribute_UsesPropertyName()
    {
        var (_, names) = ProjectionBuilder.Build<ProductTestEntity>(
            x => new { x.Name });

        Assert.Equal("Name", names["#proj0"]);
    }

    // ──────────────────────────────────────────────
    //  New tests
    // ──────────────────────────────────────────────

    [Fact]
    public void Build_SingleValueTypeProperty_Int_TriggersConvertUnwrap()
    {
        // Value types get wrapped in Convert(expr, Object) by the compiler.
        // ProjectionBuilder must unwrap this.
        var (expr, names) = ProjectionBuilder.Build<AllTypesTestEntity>(
            x => x.IntValue);

        Assert.Equal("#proj0", expr);
        Assert.Single(names);
        Assert.Equal("IntValue", names["#proj0"]);
    }

    [Fact]
    public void Build_SingleValueTypeProperty_Bool_TriggersConvertUnwrap()
    {
        var (expr, names) = ProjectionBuilder.Build<ProductTestEntity>(
            x => x.InStock);

        Assert.Equal("#proj0", expr);
        Assert.Single(names);
        Assert.Equal("InStock", names["#proj0"]);
    }

    [Fact]
    public void Build_SingleValueTypeProperty_Decimal_TriggersConvertUnwrap()
    {
        var (expr, names) = ProjectionBuilder.Build<ProductTestEntity>(
            x => x.Price);

        Assert.Equal("#proj0", expr);
        Assert.Single(names);
        Assert.Equal("Price", names["#proj0"]);
    }

    [Fact]
    public void Build_ThreeOrMoreProperties()
    {
        var (expr, names) = ProjectionBuilder.Build<OrderTestEntity>(
            x => new { x.OrderId, x.SortKey, x.Status, x.Total, x.CustomerName });

        Assert.Equal("#proj0, #proj1, #proj2, #proj3, #proj4", expr);
        Assert.Equal(5, names.Count);
        Assert.Equal("order_id", names["#proj0"]);
        Assert.Equal("sort_key", names["#proj1"]);
        Assert.Equal("status", names["#proj2"]);
        Assert.Equal("total", names["#proj3"]);
        Assert.Equal("customer_name", names["#proj4"]);
    }

    [Fact]
    public void Build_HashKeyProperty_UsesHashKeyAttributeName()
    {
        var (_, names) = ProjectionBuilder.Build<OrderTestEntity>(
            x => x.OrderId);

        Assert.Equal("order_id", names["#proj0"]);
    }

    [Fact]
    public void Build_RangeKeyProperty_UsesRangeKeyAttributeName()
    {
        var (_, names) = ProjectionBuilder.Build<OrderTestEntity>(
            x => x.SortKey);

        Assert.Equal("sort_key", names["#proj0"]);
    }

    [Fact]
    public void Build_VersionProperty_UsesPropertyName()
    {
        // Version property does not have [DynamoDBProperty(AttributeName = ...)] so it uses
        // the property name "Version"
        var (_, names) = ProjectionBuilder.Build<OrderTestEntity>(
            x => x.Version);

        Assert.Equal("Version", names["#proj0"]);
    }

    [Fact]
    public void Build_SimpleHashKeyEntity_PropertyWithoutAttributeName()
    {
        // SimpleHashKeyEntity has [DynamoDBHashKey] without custom AttributeName
        var (expr, names) = ProjectionBuilder.Build<SimpleHashKeyEntity>(
            x => x.Id);

        Assert.Equal("#proj0", expr);
        Assert.Equal("Id", names["#proj0"]);
    }
}
