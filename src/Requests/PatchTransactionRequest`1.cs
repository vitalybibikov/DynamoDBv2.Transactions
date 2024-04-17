using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions.Requests;

public class PatchTransactionRequest<T> : TransactionRequest
{
    public string? UpdateExpression { get; set; }

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

    public PatchTransactionRequest(string keyValue, Property value)
        : base(typeof(T))
    {
        var attributeValue = DynamoDbMapper.GetAttributeValue(value.Value!);
        var propertyName = DynamoDbMapper.GetPropertyAttributedName(ItemType, value.Name!);
        var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));

        Setup(keyValue, attributeValue, key, propertyName);
    }

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

        Setup(keyValue.S, attributeValue, key, propertyAttributedName);
    }

    public override Operation GetOperation()
    {
        var update = new Update
        {
            TableName = TableName,
            Key = Key,
            UpdateExpression = UpdateExpression,
            ConditionExpression = ConditionExpression,
            ExpressionAttributeNames = ExpressionAttributeNames,
            ExpressionAttributeValues = ExpressionAttributeValues
        };

        return Operation.Patch(update);
    }

    private void Init(string propertyName, AttributeValue val)
    {
        UpdateExpression = $"SET #Property = :newValue";
        ExpressionAttributeNames.Add($"#Property", $"{propertyName}");
        ExpressionAttributeValues.Add(":newValue", val);
    }

    private void Setup(string keyValue, AttributeValue? attributeValue, string key, string propertyName)
    {
        if (attributeValue != null)
        {
            Key = GetKey(new KeyValue
            {
                Key = key,
                Value = keyValue
            });

            Init(propertyName, attributeValue);
        }
    }
}