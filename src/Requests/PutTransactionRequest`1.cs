using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions.Requests;

public sealed class PutTransactionRequest<T> : TransactionRequest
{
    public PutItemRequest PutRequest { get; set; }
    public override TransactOperationType Type => TransactOperationType.Put;

    public PutTransactionRequest(T item)
        : base(typeof(T))
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        var convertedItem = DynamoDbMapper.MapToAttribute(item);
        var (propertyName, value) = DynamoDbMapper.GetVersion(item);
        var attributeName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));

        var keyFound = convertedItem.TryGetValue(attributeName, out AttributeValue? keyValue);

        if (!keyFound)
        {
            throw new ArgumentNullException("HashKey not found in the model");
        }

        SetVersion<T>(convertedItem, propertyName);
        PutRequest = new PutItemRequest(TableName, convertedItem);
    }

    public override Operation GetOperation()
    {
        var put = new Put
        {
            TableName = TableName,
            Item = PutRequest.Item,
            ConditionExpression = ConditionExpression,
            ExpressionAttributeNames = ExpressionAttributeNames,
            ExpressionAttributeValues = ExpressionAttributeValues
        };

        return Operation.Put(put);
    }
}