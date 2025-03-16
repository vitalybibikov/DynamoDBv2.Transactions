using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;

namespace DynamoDBv2.Transactions.Requests;

/// <summary>
/// Represents a request to perform a <c>PutItem</c> operation.
/// </summary>
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
        var (propertyName, _) = DynamoDbMapper.GetVersion(item);

        SetVersion<T>(convertedItem, propertyName);
        PutRequest = new PutItemRequest(TableName, convertedItem);
    }

    public override Operation GetOperation()
    {
        var put = new Put
        {
            TableName = TableName,
            Item = PutRequest.Item
        };

        if (ExpressionAttributeNames.Any())
        {
            put.ExpressionAttributeNames = ExpressionAttributeNames;
        }

        if (ExpressionAttributeValues.Any())
        {
            put.ExpressionAttributeValues = ExpressionAttributeValues;
        }

        if (!String.IsNullOrEmpty(ConditionExpression))
        {
            put.ConditionExpression = ConditionExpression;
        }

        return Operation.Put(put);
    }
}