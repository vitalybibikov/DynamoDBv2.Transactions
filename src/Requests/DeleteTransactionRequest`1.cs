using Amazon.DynamoDBv2.DataModel;
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
        SetKey(key);
    }

    public DeleteTransactionRequest(KeyValue keyValue)
        : base(typeof(T))
    {
        SetKey(GetKey(keyValue));
    }

    /// <summary>
    /// Delete item by its HASH key value, assumes that <see cref="DynamoDBHashKeyAttribute"/> is set.
    /// </summary>
    /// <param name="keyValue">Value of the Key</param>
    public DeleteTransactionRequest(string keyValue)
        : base(typeof(T))
    {
        var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        SetKey(GetKey(new KeyValue
        {
            Key = key,
            Value = keyValue
        }));
    }

    private void SetKey(Dictionary<string, AttributeValue> key)
    {
        Key = key;
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
