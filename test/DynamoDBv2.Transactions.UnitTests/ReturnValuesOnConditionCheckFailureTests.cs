using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for fix #10: ReturnValuesOnConditionCheckFailure property
/// is propagated from request objects to the underlying AWS operation types.
/// </summary>
public class ReturnValuesOnConditionCheckFailureTests
{
    [Fact]
    public void DeleteRequest_ReturnValuesOnConditionCheckFailure_SetOnOperation()
    {
        // Arrange
        var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("key-1");
        request.ReturnValuesOnConditionCheckFailure =
            ReturnValuesOnConditionCheckFailure.ALL_OLD;

        // Act
        var operation = request.GetOperation();

        // Assert
        Assert.NotNull(operation.DeleteType);
        Assert.Equal(
            ReturnValuesOnConditionCheckFailure.ALL_OLD,
            operation.DeleteType!.ReturnValuesOnConditionCheckFailure);
    }

    [Fact]
    public void PutRequest_ReturnValuesOnConditionCheckFailure_SetOnOperation()
    {
        // Arrange
        var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test" };
        var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);
        request.ReturnValuesOnConditionCheckFailure =
            ReturnValuesOnConditionCheckFailure.ALL_OLD;

        // Act
        var operation = request.GetOperation();

        // Assert
        Assert.NotNull(operation.PutType);
        Assert.Equal(
            ReturnValuesOnConditionCheckFailure.ALL_OLD,
            operation.PutType!.ReturnValuesOnConditionCheckFailure);
    }

    [Fact]
    public void PatchRequest_ReturnValuesOnConditionCheckFailure_SetOnOperation()
    {
        // Arrange
        var request = new PatchTransactionRequest<SomeDynamoDbEntity>(
            new KeyValue { Key = "Id", Value = "1" },
            new Property { Name = "Status", Value = "Updated" });
        request.ReturnValuesOnConditionCheckFailure =
            ReturnValuesOnConditionCheckFailure.ALL_OLD;

        // Act
        var operation = request.GetOperation();

        // Assert — PatchTransactionRequest produces an Update operation
        Assert.NotNull(operation.UpdateType);
        Assert.Equal(
            ReturnValuesOnConditionCheckFailure.ALL_OLD,
            operation.UpdateType!.ReturnValuesOnConditionCheckFailure);
    }

    [Fact]
    public void UpdateRequest_ReturnValuesOnConditionCheckFailure_SetOnOperation()
    {
        // Arrange
        var request = new UpdateTransactionRequest<SomeDynamoDbEntity>("SET #a = :v");
        request.ReturnValuesOnConditionCheckFailure =
            ReturnValuesOnConditionCheckFailure.ALL_OLD;

        // Act
        var operation = request.GetOperation();

        // Assert
        Assert.NotNull(operation.UpdateType);
        Assert.Equal(
            ReturnValuesOnConditionCheckFailure.ALL_OLD,
            operation.UpdateType!.ReturnValuesOnConditionCheckFailure);
    }

    [Fact]
    public void ConditionCheckRequest_ReturnValuesOnConditionCheckFailure_SetOnOperation()
    {
        // Arrange
        var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key-1");
        request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");
        request.ReturnValuesOnConditionCheckFailure =
            ReturnValuesOnConditionCheckFailure.ALL_OLD;

        // Act
        var operation = request.GetOperation();

        // Assert
        Assert.NotNull(operation.ConditionCheckType);
        Assert.Equal(
            ReturnValuesOnConditionCheckFailure.ALL_OLD,
            operation.ConditionCheckType!.ReturnValuesOnConditionCheckFailure);
    }

    [Fact]
    public void AllRequests_DefaultNull_NotSet()
    {
        // Arrange — create all request types without setting ReturnValuesOnConditionCheckFailure
        var deleteRequest = new DeleteTransactionRequest<SomeDynamoDbEntity>("key-1");
        var putRequest = new PutTransactionRequest<SomeDynamoDbEntity>(
            new SomeDynamoDbEntity { Id = "1", Name = "Test" });
        var patchRequest = new PatchTransactionRequest<SomeDynamoDbEntity>(
            new KeyValue { Key = "Id", Value = "1" },
            new Property { Name = "Status", Value = "OK" });
        var updateRequest = new UpdateTransactionRequest<SomeDynamoDbEntity>("SET #a = :v");
        var conditionRequest = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key-1");
        conditionRequest.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");

        // Act
        var deleteOp = deleteRequest.GetOperation();
        var putOp = putRequest.GetOperation();
        var patchOp = patchRequest.GetOperation();
        var updateOp = updateRequest.GetOperation();
        var conditionOp = conditionRequest.GetOperation();

        // Assert — by default, ReturnValuesOnConditionCheckFailure should be null/not set
        Assert.Null(deleteRequest.ReturnValuesOnConditionCheckFailure);
        Assert.Null(putRequest.ReturnValuesOnConditionCheckFailure);
        Assert.Null(patchRequest.ReturnValuesOnConditionCheckFailure);
        Assert.Null(updateRequest.ReturnValuesOnConditionCheckFailure);
        Assert.Null(conditionRequest.ReturnValuesOnConditionCheckFailure);

        // The AWS model objects should also not have it set
        Assert.Null(deleteOp.DeleteType!.ReturnValuesOnConditionCheckFailure);
        Assert.Null(putOp.PutType!.ReturnValuesOnConditionCheckFailure);
        Assert.Null(patchOp.UpdateType!.ReturnValuesOnConditionCheckFailure);
        Assert.Null(updateOp.UpdateType!.ReturnValuesOnConditionCheckFailure);
        Assert.Null(conditionOp.ConditionCheckType!.ReturnValuesOnConditionCheckFailure);
    }
}
