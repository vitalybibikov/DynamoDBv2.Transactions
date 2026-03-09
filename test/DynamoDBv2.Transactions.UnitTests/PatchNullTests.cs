using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests that PatchTransactionRequest correctly handles null property values
/// by setting an explicit DynamoDB NULL attribute (fix #12: patch null support).
/// Uses ProductTestEntity (hash-only, has Name as a string property).
/// </summary>
public class PatchNullTests
{
    [Fact]
    public void PatchTransactionRequest_ModelBased_NullProperty_SetsNullAttributeValue()
    {
        // Name is null — MapToAttribute will skip it, so the patch constructor
        // should fall back to an explicit NULL AttributeValue.
        var entity = new ProductTestEntity { ProductId = "prod-1", Name = null! };

        var request = new PatchTransactionRequest<ProductTestEntity>(entity, "Name");

        // The expression attribute values should contain :newValue with NULL = true
        Assert.True(request.ExpressionAttributeValues.ContainsKey(":newValue"));
        Assert.True(request.ExpressionAttributeValues[":newValue"].NULL);
    }

    [Fact]
    public void PatchTransactionRequest_ModelBased_NullProperty_HasUpdateExpression()
    {
        var entity = new ProductTestEntity { ProductId = "prod-1", Name = null! };

        var request = new PatchTransactionRequest<ProductTestEntity>(entity, "Name");

        Assert.Equal("SET #Property = :newValue", request.UpdateExpression);
        Assert.True(request.ExpressionAttributeNames.ContainsKey("#Property"));
    }

    [Fact]
    public void PatchTransactionRequest_ExpressionBased_NullValue_SetsNullAttributeValue()
    {
        // Using the Setup path (string keyValue, Property value) with null Value.
        // The DynamoDbMapper.GetAttributeValue(null!) returns null, and
        // Setup handles null attributeValue by creating { NULL = true }.
        var property = new Property { Name = "Name", Value = null! };

        // This exercises the Setup path with null attributeValue
        var request = new PatchTransactionRequest<ProductTestEntity>("prod-1", property);

        Assert.True(request.ExpressionAttributeValues.ContainsKey(":newValue"));
        Assert.True(request.ExpressionAttributeValues[":newValue"].NULL);
    }

    [Fact]
    public void PatchTransactionRequest_ModelBased_NonNullProperty_StillWorks()
    {
        var entity = new ProductTestEntity { ProductId = "prod-1", Name = "Widget" };

        var request = new PatchTransactionRequest<ProductTestEntity>(entity, "Name");

        Assert.True(request.ExpressionAttributeValues.ContainsKey(":newValue"));
        var attrValue = request.ExpressionAttributeValues[":newValue"];
        // Should be a normal string value, not NULL
        Assert.NotEqual(true, attrValue.NULL);
        Assert.Equal("Widget", attrValue.S);
    }

    [Fact]
    public void PatchTransactionRequest_Setup_NullAttributeValue_SetsNullAttribute()
    {
        // The composite-key constructor uses Init which requires non-null attributeValue,
        // but when attributeValue is null (from GetAttributeValue returning null),
        // the constructor skips Init. We test the explicit NULL path through model-based.
        var entity = new ProductTestEntity { ProductId = "prod-1", Name = null! };

        var request = new PatchTransactionRequest<ProductTestEntity>(entity, "Name");

        // Verify the full round-trip through GetOperation
        var operation = request.GetOperation();
        Assert.NotNull(operation.UpdateType);
        var update = operation.UpdateType!;

        Assert.Equal("Products", update.TableName);
        Assert.Contains(":newValue", update.ExpressionAttributeValues.Keys);
        Assert.True(update.ExpressionAttributeValues[":newValue"].NULL);
        Assert.Equal("SET #Property = :newValue", update.UpdateExpression);
    }
}
