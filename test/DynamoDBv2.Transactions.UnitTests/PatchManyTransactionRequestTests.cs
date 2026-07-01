using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for <see cref="PatchManyTransactionRequest{T}"/> — multi-attribute partial update with
/// optional atomic version increment. Uses OrderTestEntity (hash + range + [DynamoDBVersion]).
/// </summary>
public class PatchManyTransactionRequestTests
{
    private static OrderTestEntity CreateOrder() => new()
    {
        OrderId = "order-1",
        SortKey = "sort-1",
        Status = "Closed",
        Total = 42.5,
        CustomerName = "Alice",
        Version = 7
    };

    private static string AttrName(string propertyName) =>
        DynamoDbMapper.GetPropertyAttributedName(typeof(OrderTestEntity), propertyName);

    [Fact]
    public void SetsOnlyListedAttributes()
    {
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(),
            new[] { "Status", "Total", "CustomerName" },
            incrementVersion: false);

        Assert.StartsWith("SET ", request.UpdateExpression);
        Assert.Contains(AttrName("Status"), request.ExpressionAttributeNames.Values);
        Assert.Contains(AttrName("Total"), request.ExpressionAttributeNames.Values);
        Assert.Contains(AttrName("CustomerName"), request.ExpressionAttributeNames.Values);

        // Three SET assignments were produced.
        Assert.Contains(":v0", request.ExpressionAttributeValues.Keys);
        Assert.Contains(":v1", request.ExpressionAttributeValues.Keys);
        Assert.Contains(":v2", request.ExpressionAttributeValues.Keys);
    }

    [Fact]
    public void TargetsTheCompositeKey()
    {
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(),
            new[] { "Status" },
            incrementVersion: false);

        Assert.Equal(2, request.Key.Count);
        Assert.Equal("order-1", request.Key["order_id"].S);
        Assert.Equal("sort-1", request.Key["sort_key"].S);
    }

    [Fact]
    public void WhenIncrementVersion_AddsAtomicIncrement_NotEqualityCondition()
    {
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(),
            new[] { "Status" },
            incrementVersion: true);

        // Version is incremented via ADD, never SET, and there is no version equality condition.
        Assert.Contains("ADD #version :increment", request.UpdateExpression);
        var addIndex = request.UpdateExpression!.IndexOf("ADD", StringComparison.Ordinal);
        var (versionAttr, _) = DynamoDbMapper.GetVersion(CreateOrder());
        Assert.DoesNotContain(versionAttr!, request.UpdateExpression.Substring(0, addIndex));
        Assert.Equal("1", request.ExpressionAttributeValues[":increment"].N);
        Assert.Equal(versionAttr, request.ExpressionAttributeNames["#version"]);
        Assert.DoesNotContain(":expectedVersion", request.ExpressionAttributeValues.Keys);
    }

    [Fact]
    public void WhenNotIncrementVersion_HasNoVersionTokens()
    {
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(),
            new[] { "Status", "Total" },
            incrementVersion: false);

        Assert.DoesNotContain("ADD", request.UpdateExpression!);
        Assert.DoesNotContain("#version", request.ExpressionAttributeNames.Keys);
        Assert.DoesNotContain(":increment", request.ExpressionAttributeValues.Keys);
    }

    [Fact]
    public void RequiresItemToExist()
    {
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(),
            new[] { "Status" },
            incrementVersion: true);

        Assert.Equal("attribute_exists(#hashKey)", request.ConditionExpression);
        Assert.Equal(DynamoDbMapper.GetHashKeyAttributeName(typeof(OrderTestEntity)), request.ExpressionAttributeNames["#hashKey"]);
    }

    [Fact]
    public void CarriesTheProvidedValues()
    {
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(),
            new[] { "Status", "Total" },
            incrementVersion: false);

        // Map property -> its value token by resolving the name token position.
        var statusValueToken = ResolveValueToken(request, AttrName("Status"));
        var totalValueToken = ResolveValueToken(request, AttrName("Total"));

        Assert.Equal("Closed", request.ExpressionAttributeValues[statusValueToken].S);
        Assert.Equal("42.5", request.ExpressionAttributeValues[totalValueToken].N);
    }

    [Fact]
    public void EmptyPropertyNames_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new PatchManyTransactionRequest<OrderTestEntity>(CreateOrder(), Array.Empty<string>(), incrementVersion: false));
    }

    [Fact]
    public void IncrementVersion_OnTypeWithoutVersion_Throws()
    {
        var product = new ProductTestEntity { ProductId = "prod-1", Name = "Widget", Price = 1m, InStock = true };

        Assert.Throws<InvalidOperationException>(() =>
            new PatchManyTransactionRequest<ProductTestEntity>(product, new[] { "Name" }, incrementVersion: true));
    }

    [Fact]
    public void HashKeyOnlyEntity_TargetsSingleKey()
    {
        var product = new ProductTestEntity { ProductId = "prod-1", Name = "Widget", Price = 1m, InStock = true };

        var request = new PatchManyTransactionRequest<ProductTestEntity>(
            product,
            new[] { "Name", "Price" },
            incrementVersion: false);

        Assert.Single(request.Key);
        Assert.Equal("prod-1", request.Key["product_id"].S);
    }

    [Fact]
    public void NullModel_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new PatchManyTransactionRequest<OrderTestEntity>(null!, new[] { "Status" }, incrementVersion: false));
    }

    [Fact]
    public void NullPropertyNames_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new PatchManyTransactionRequest<OrderTestEntity>(CreateOrder(), null!, incrementVersion: false));
    }

    [Fact]
    public void MissingHashKeyValue_Throws()
    {
        // A null hash-key property is skipped by MapToAttribute, so the key cannot be built.
        var order = CreateOrder();
        order.OrderId = null!;

        Assert.Throws<ArgumentException>(() =>
            new PatchManyTransactionRequest<OrderTestEntity>(order, new[] { "Status" }, incrementVersion: false));
    }

    [Fact]
    public void NullValuedAttribute_IsPatchedAsExplicitNull()
    {
        // Name is null -> MapToAttribute skips it -> the patch must still SET it, as an explicit NULL.
        var product = new ProductTestEntity { ProductId = "prod-1", Name = null!, Price = 1m, InStock = true };

        var request = new PatchManyTransactionRequest<ProductTestEntity>(
            product,
            new[] { "Name" },
            incrementVersion: false);

        var nameAttr = DynamoDbMapper.GetPropertyAttributedName(typeof(ProductTestEntity), "Name");
        string? valueToken = null;
        foreach (var pair in request.ExpressionAttributeNames)
        {
            if (pair.Value == nameAttr)
            {
                valueToken = ":v" + pair.Key.Substring(2);
            }
        }

        Assert.NotNull(valueToken);
        Assert.True(request.ExpressionAttributeValues[valueToken!].NULL);
    }

    [Fact]
    public void GetOperation_ReturnsUpdateWithExpressionsAndKey()
    {
        var request = new PatchManyTransactionRequest<OrderTestEntity>(
            CreateOrder(),
            new[] { "Status", "Total" },
            incrementVersion: true);

        var operation = request.GetOperation();

        Assert.NotNull(operation);
        Assert.IsType<Update>(operation.UpdateType);
        var update = operation.UpdateType;
        Assert.Equal(request.TableName, update.TableName);
        Assert.Equal(request.UpdateExpression, update.UpdateExpression);
        Assert.Equal("attribute_exists(#hashKey)", update.ConditionExpression);
        Assert.Equal(2, update.Key.Count);
        Assert.True(update.ExpressionAttributeNames.Count > 0);
        Assert.True(update.ExpressionAttributeValues.Count > 0);
    }

    private static string ResolveValueToken(PatchManyTransactionRequest<OrderTestEntity> request, string attributeName)
    {
        foreach (var pair in request.ExpressionAttributeNames)
        {
            if (pair.Value == attributeName)
            {
                // name token "#pN" -> value token ":vN"
                return ":v" + pair.Key.Substring(2);
            }
        }

        throw new KeyNotFoundException(attributeName);
    }
}
