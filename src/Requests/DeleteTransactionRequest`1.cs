using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions.Requests;

/// <summary>
/// Represents a request to perform a <c>DeleteItem</c> operation.
/// </summary>
public sealed class DeleteTransactionRequest<T> : TransactionRequest
{
    public DeleteTransactionRequest(Dictionary<string, AttributeValue> key)
        : base(typeof(T))
    {
        Key = key;
    }

    public DeleteTransactionRequest(KeyValue keyValue)
        : base(typeof(T))
    {
        Key = GetKey(keyValue);
    }

    public DeleteTransactionRequest(string keyValue)
        : base(typeof(T))
    {
        var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        Key = GetKey(new KeyValue
        {
            Key = key,
            Value = keyValue
        });
    }

    public override TransactOperationType Type => TransactOperationType.Delete;

    public override Operation GetOperation()
    {
        var delete = new Delete
        {
            TableName = TableName,
            Key = Key,
            ConditionExpression = ConditionExpression,
            ExpressionAttributeNames = ExpressionAttributeNames,
            ExpressionAttributeValues = ExpressionAttributeValues
        };

        return Operation.Delete(delete);
    }
}