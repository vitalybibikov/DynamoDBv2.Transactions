using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;

namespace DynamoDBv2.Transactions.Requests;

/// <summary>
/// Represents a request to perform an <c>UpdateItem</c> operation.
/// </summary>
public sealed class UpdateTransactionRequest<T> : TransactionRequest
{
    public UpdateTransactionRequest(string? expression = default)
        : base(typeof(T))
    {
        UpdateExpression = expression!;
    }

    public string UpdateExpression { get; set; }

    public override TransactOperationType Type => TransactOperationType.Update;

    public override Operation GetOperation()
    {
        var update = new Update
        {
            TableName = TableName,
            Key = Key
        };

        if (ExpressionAttributeNames.Count > 0)
        {
            update.ExpressionAttributeNames = ExpressionAttributeNames;
        }

        if (ExpressionAttributeValues.Count > 0)
        {
            update.ExpressionAttributeValues = ExpressionAttributeValues;
        }

        if (!String.IsNullOrEmpty(ConditionExpression))
        {
            update.ConditionExpression = ConditionExpression;
        }

        if (!String.IsNullOrEmpty(UpdateExpression))
        {
            update.UpdateExpression = UpdateExpression;
        }

        if (ReturnValuesOnConditionCheckFailure != null)
        {
            update.ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure;
        }

        return Operation.Update(update);
    }
}