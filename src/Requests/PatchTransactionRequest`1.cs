using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions.Requests;

/// <summary>
/// Represents a request to perform a <c>Patch</c> operation.
/// </summary>
public sealed class PatchTransactionRequest<T> : TransactionRequest
{
    public string? UpdateExpression { get; private set; }

    public override TransactOperationType Type => TransactOperationType.Patch;

    public PatchTransactionRequest(KeyValue keyValue, Property value)
        : base(typeof(T))
    {
        var val = DynamoDbMapper.GetAttributeValue(value.Value!);
        var propertyName = DynamoDbMapper.GetPropertyAttributedName(ItemType, value.Name);

        if (val != null)
        {
            Key = GetKey(keyValue);
            Init(propertyName, val);
        }
    }

    /// <summary>
    /// Creates a new <see cref="PatchTransactionRequest{T}"/> instance. Assuming
    /// that HASH key is marked with <see cref="DynamoDBHashKeyAttribute"/>.
    /// </summary>
    /// <param name="keyValue">HASH key value </param>
    /// <param name="value">Property to be patched</param>
    public PatchTransactionRequest(string keyValue, Property value)
        : base(typeof(T))
    {
        var attributeValue = DynamoDbMapper.GetAttributeValue(value.Value!);
        var propertyName = DynamoDbMapper.GetPropertyAttributedName(ItemType, value.Name!);
        var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));

        var keyAttribute = new AttributeValue { S = keyValue };

        Setup(key, keyAttribute, attributeValue, propertyName);
    }

    /// <summary>
    /// Creates a new <see cref="PatchTransactionRequest{T}"/> instance with composite key.
    /// </summary>
    /// <param name="hashKeyValue">Value of the hash key.</param>
    /// <param name="rangeKeyValue">Value of the range key.</param>
    /// <param name="value">Property to be patched.</param>
    public PatchTransactionRequest(string hashKeyValue, string rangeKeyValue, Property value)
        : base(typeof(T))
    {
        var attributeValue = DynamoDbMapper.GetAttributeValue(value.Value!);
        var propertyName = DynamoDbMapper.GetPropertyAttributedName(ItemType, value.Name!);
        var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        var rangeKeyName = DynamoDbMapper.GetRangeKeyAttributeName(typeof(T));

        Key = new Dictionary<string, AttributeValue>
        {
            { hashKeyName, new AttributeValue { S = hashKeyValue } },
            { rangeKeyName, new AttributeValue { S = rangeKeyValue } }
        };

        if (attributeValue != null)
        {
            Init(propertyName, attributeValue);
        }
    }

    /// <summary>
    /// Creates a new <see cref="PatchTransactionRequest{T}"/> instance. Assuming
    /// that HASH key is marked with <see cref="DynamoDBHashKeyAttribute"/>.
    /// </summary>
    /// <param name="model">Type to be patched</param>
    /// <param name="propertyName">Property that is going to be patched. </param>
    /// <exception cref="ArgumentNullException">exception</exception>
    public PatchTransactionRequest(T model, string propertyName)
        : base(typeof(T))
    {
        if (model == null)
        {
            throw new ArgumentNullException(nameof(model));
        }

        if (propertyName == null)
        {
            throw new ArgumentNullException(nameof(propertyName));
        }

        var attributes = DynamoDbMapper.MapToAttribute(model);
        var propertyAttributedName = DynamoDbMapper.GetPropertyAttributedName(ItemType, propertyName);
        var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));

        if (!attributes.TryGetValue(key, out var keyValue))
        {
            throw new ArgumentException($"Hash key '{key}' not found in mapped attributes for type {typeof(T).Name}. Ensure the hash key property is non-null.");
        }

        if (!attributes.TryGetValue(propertyAttributedName, out var attributeValue))
        {
            // Property value is null — MapToAttribute skips nulls, so use explicit NULL
            attributeValue = new AttributeValue { NULL = true };
        }

        Setup(key, keyValue, attributeValue, propertyAttributedName);
    }

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

        return Operation.Patch(update);
    }

    private void Init(string propertyName, AttributeValue val)
    {
        UpdateExpression = $"SET #Property = :newValue";
        ExpressionAttributeNames.Add($"#Property", $"{propertyName}");
        ExpressionAttributeValues.Add(":newValue", val);
    }

    private void Setup(string keyName, AttributeValue keyValue, AttributeValue? attributeValue, string propertyName)
    {
        var key = new Dictionary<string, AttributeValue> { { keyName, keyValue } };
        Key = key;
        Init(propertyName, attributeValue ?? new AttributeValue { NULL = true });
    }
}