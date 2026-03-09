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

        // Populate Key for duplicate detection in TransactionManager (best-effort)
        PopulateKeyForDuplicateDetection(convertedItem);
    }

    public override Operation GetOperation()
    {
        var put = new Put
        {
            TableName = TableName,
            Item = PutRequest.Item
        };

        if (ExpressionAttributeNames.Count > 0)
        {
            put.ExpressionAttributeNames = ExpressionAttributeNames;
        }

        if (ExpressionAttributeValues.Count > 0)
        {
            put.ExpressionAttributeValues = ExpressionAttributeValues;
        }

        if (!String.IsNullOrEmpty(ConditionExpression))
        {
            put.ConditionExpression = ConditionExpression;
        }

        if (ReturnValuesOnConditionCheckFailure != null)
        {
            put.ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure;
        }

        return Operation.Put(put);
    }

    private void PopulateKeyForDuplicateDetection(Dictionary<string, AttributeValue> convertedItem)
    {
        try
        {
            var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
            if (convertedItem.TryGetValue(hashKeyName, out var hashKeyAttr))
            {
                Key[hashKeyName] = hashKeyAttr;
            }

            var rangeKeyName = DynamoDbMapper.TryGetRangeKeyAttributeName(typeof(T));
            if (rangeKeyName != null && convertedItem.TryGetValue(rangeKeyName, out var rangeKeyAttr))
            {
                Key[rangeKeyName] = rangeKeyAttr;
            }
        }
        catch (ArgumentException)
        {
            // Type doesn't have DynamoDB key attributes — skip key extraction
        }
    }
}
