using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions.Requests.Contract;

/// <summary>
/// Contract for a DynamoDB transactional get request.
/// </summary>
public interface IGetTransactionRequest
{
    string TableName { get; }
    Dictionary<string, AttributeValue> Key { get; }
    string? ProjectionExpression { get; }
    Dictionary<string, string> ExpressionAttributeNames { get; }
    Type ItemType { get; }
}
