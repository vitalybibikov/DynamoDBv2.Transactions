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

        var keyValue = attributes[key];
        var attributeValue = attributes[propertyAttributedName];

        Setup(key, keyValue, attributeValue, propertyAttributedName);
    }

    public override Operation GetOperation()
    {
        var update = new Update
        {
            TableName = TableName,
            Key = Key
        };

        if (ExpressionAttributeNames.Any())
        {
            update.ExpressionAttributeNames = ExpressionAttributeNames;
        }

        if (ExpressionAttributeValues.Any())
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
        if (attributeValue != null)
        {
            var key = new Dictionary<string, AttributeValue> { { keyName, keyValue } };

            Key = key;
            Init(propertyName, attributeValue);
        }
    }
}