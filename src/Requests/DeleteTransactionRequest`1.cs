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
        var keyNameAttributed = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        Key = new Dictionary<string, AttributeValue>
        {
            { keyNameAttributed, new AttributeValue { S = keyValue } }
        };
    }

    /// <summary>
    /// Delete item by its HASH key value (supports String, Number, or Binary key types).
    /// </summary>
    /// <param name="keyValue">Value of the Key (string, int, long, decimal, double, float, or byte[])</param>
    public DeleteTransactionRequest(object keyValue)
        : base(typeof(T))
    {
        var keyNameAttributed = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        Key = new Dictionary<string, AttributeValue>
        {
            { keyNameAttributed, ToKeyAttributeValue(keyValue) }
        };
    }

    /// <summary>
    /// Delete item by its HASH + RANGE key values.
    /// </summary>
    /// <param name="hashKeyValue">Value of the hash key.</param>
    /// <param name="rangeKeyValue">Value of the range key.</param>
    public DeleteTransactionRequest(string hashKeyValue, string rangeKeyValue)
        : base(typeof(T))
    {
        var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        var rangeKeyName = DynamoDbMapper.GetRangeKeyAttributeName(typeof(T));
        Key = new Dictionary<string, AttributeValue>
        {
            { hashKeyName, new AttributeValue { S = hashKeyValue } },
            { rangeKeyName, new AttributeValue { S = rangeKeyValue } }
        };
    }

    /// <summary>
    /// Delete item by its HASH + RANGE key values (supports String, Number, or Binary key types).
    /// </summary>
    /// <param name="hashKeyValue">Value of the hash key (string, int, long, decimal, double, float, or byte[])</param>
    /// <param name="rangeKeyValue">Value of the range key (string, int, long, decimal, double, float, or byte[])</param>
    public DeleteTransactionRequest(object hashKeyValue, object rangeKeyValue)
        : base(typeof(T))
    {
        var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        var rangeKeyName = DynamoDbMapper.GetRangeKeyAttributeName(typeof(T));
        Key = new Dictionary<string, AttributeValue>
        {
            { hashKeyName, ToKeyAttributeValue(hashKeyValue) },
            { rangeKeyName, ToKeyAttributeValue(rangeKeyValue) }
        };
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
            Key = Key
        };

        if (ExpressionAttributeNames.Count > 0)
        {
            delete.ExpressionAttributeNames = ExpressionAttributeNames;
        }

        if (ExpressionAttributeValues.Count > 0)
        {
            delete.ExpressionAttributeValues = ExpressionAttributeValues;
        }

        if (!String.IsNullOrEmpty(ConditionExpression))
        {
            delete.ConditionExpression = ConditionExpression;
        }

        if (ReturnValuesOnConditionCheckFailure != null)
        {
            delete.ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure;
        }

        return Operation.Delete(delete);
    }
}
