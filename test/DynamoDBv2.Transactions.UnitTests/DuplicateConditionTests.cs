using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests that adding multiple conditions on the same property produces
/// unique tokens and preserves all values (fix for duplicate condition collision).
/// Uses SomeDynamoDbEntity which has Amount (double) and Status (string).
/// </summary>
public class DuplicateConditionTests
{
    [Fact]
    public void AddCondition_SamePropertyTwice_BothValuesPreserved()
    {
        var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key-1");

        // Amount > 10 AND Amount < 100
        request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 10.0);
        request.LessThan<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);

        // Two distinct value tokens should exist
        Assert.Equal(2, request.ExpressionAttributeValues.Count);

        // Both values should be present
        var values = request.ExpressionAttributeValues.Values.Select(v => v.N).ToList();
        Assert.Contains("10", values);
        Assert.Contains("100", values);
    }

    [Fact]
    public void AddCondition_SamePropertyTwice_ExpressionCorrect()
    {
        var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key-1");

        request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 10.0);
        request.LessThan<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);

        // Force the condition expression to be built
        var operation = request.GetOperation();
        var check = operation.ConditionCheckType!;

        // Should contain two separate condition clauses with different tokens
        Assert.Contains("#p0", check.ConditionExpression);
        Assert.Contains("#p1", check.ConditionExpression);
        Assert.Contains(":v0", check.ConditionExpression);
        Assert.Contains(":v1", check.ConditionExpression);
        Assert.Contains(">", check.ConditionExpression);
        Assert.Contains("<", check.ConditionExpression);
    }

    [Fact]
    public void AddCondition_ThreeConditions_SameProperty_AllPreserved()
    {
        var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key-1");

        request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 5.0);
        request.LessThan<SomeDynamoDbEntity, double>(x => x.Amount, 200.0);
        request.NotEquals<SomeDynamoDbEntity, double>(x => x.Amount, 50.0);

        Assert.Equal(3, request.ExpressionAttributeValues.Count);
        Assert.Equal(3, request.ExpressionAttributeNames.Count);

        // Each condition gets a unique counter token
        Assert.True(request.ExpressionAttributeNames.ContainsKey("#p0"));
        Assert.True(request.ExpressionAttributeNames.ContainsKey("#p1"));
        Assert.True(request.ExpressionAttributeNames.ContainsKey("#p2"));
        Assert.True(request.ExpressionAttributeValues.ContainsKey(":v0"));
        Assert.True(request.ExpressionAttributeValues.ContainsKey(":v1"));
        Assert.True(request.ExpressionAttributeValues.ContainsKey(":v2"));
    }

    [Fact]
    public void AddCondition_MixedProperties_NoCollision()
    {
        var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key-1");

        // Two conditions on Amount, one on Status
        request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 10.0);
        request.LessThan<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);
        request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");

        Assert.Equal(3, request.ExpressionAttributeValues.Count);
        Assert.Equal(3, request.ExpressionAttributeNames.Count);

        // Amount should appear twice in name tokens, Status once
        var nameValues = request.ExpressionAttributeNames.Values.ToList();
        Assert.Equal(2, nameValues.Count(v => v == "Amount"));
        Assert.Equal(1, nameValues.Count(v => v == "Status"));
    }

    [Fact]
    public void GetOperation_DuplicateConditions_AllTokensInExpression()
    {
        var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key-1");

        request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 10.0);
        request.LessThan<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);

        var operation = request.GetOperation();
        var check = operation.ConditionCheckType!;

        // The final expression should reference all tokens
        foreach (var nameToken in request.ExpressionAttributeNames.Keys)
        {
            Assert.Contains(nameToken, check.ConditionExpression);
        }

        foreach (var valueToken in request.ExpressionAttributeValues.Keys)
        {
            Assert.Contains(valueToken, check.ConditionExpression);
        }

        // Should be joined with AND
        Assert.Contains("AND", check.ConditionExpression);
    }
}
