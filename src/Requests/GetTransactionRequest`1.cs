using System.Linq.Expressions;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions.Requests;

/// <summary>
/// Represents a request to perform a transactional <c>Get</c> operation.
/// </summary>
public sealed class GetTransactionRequest<T> : IGetTransactionRequest
    where T : class, new()
{
    public string TableName { get; }
    public Dictionary<string, AttributeValue> Key { get; }
    public string? ProjectionExpression { get; private set; }
    public Dictionary<string, string> ExpressionAttributeNames { get; } = new();
    public Type ItemType => typeof(T);

    /// <summary>
    /// Creates a get request for an item by its hash key value.
    /// Assumes that the hash key is marked with <see cref="Amazon.DynamoDBv2.DataModel.DynamoDBHashKeyAttribute"/>.
    /// </summary>
    /// <param name="hashKeyValue">The hash key value.</param>
    public GetTransactionRequest(string hashKeyValue)
    {
        ArgumentNullException.ThrowIfNull(hashKeyValue);

        TableName = DynamoDbMapper.GetTableName(typeof(T));
        var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        Key = new Dictionary<string, AttributeValue>(1)
        {
            { hashKeyName, new AttributeValue { S = hashKeyValue } }
        };
    }

    /// <summary>
    /// Creates a get request for an item by its composite key (hash + range).
    /// </summary>
    /// <param name="hashKeyValue">The hash key value.</param>
    /// <param name="rangeKeyValue">The range key value.</param>
    public GetTransactionRequest(string hashKeyValue, string rangeKeyValue)
    {
        ArgumentNullException.ThrowIfNull(hashKeyValue);
        ArgumentNullException.ThrowIfNull(rangeKeyValue);

        TableName = DynamoDbMapper.GetTableName(typeof(T));
        var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        var rangeKeyName = DynamoDbMapper.GetRangeKeyAttributeName(typeof(T));

        Key = new Dictionary<string, AttributeValue>(2)
        {
            { hashKeyName, new AttributeValue { S = hashKeyValue } },
            { rangeKeyName, new AttributeValue { S = rangeKeyValue } }
        };
    }

    /// <summary>
    /// Creates a get request with a projection expression.
    /// </summary>
    /// <param name="hashKeyValue">The hash key value.</param>
    /// <param name="projection">Lambda selecting properties to retrieve.</param>
    public GetTransactionRequest(string hashKeyValue, Expression<Func<T, object>> projection)
        : this(hashKeyValue)
    {
        ArgumentNullException.ThrowIfNull(projection);
        ApplyProjection(projection);
    }

    /// <summary>
    /// Creates a get request with composite key and projection expression.
    /// </summary>
    /// <param name="hashKeyValue">The hash key value.</param>
    /// <param name="rangeKeyValue">The range key value.</param>
    /// <param name="projection">Lambda selecting properties to retrieve.</param>
    public GetTransactionRequest(string hashKeyValue, string rangeKeyValue, Expression<Func<T, object>> projection)
        : this(hashKeyValue, rangeKeyValue)
    {
        ArgumentNullException.ThrowIfNull(projection);
        ApplyProjection(projection);
    }

    private void ApplyProjection(Expression<Func<T, object>> projection)
    {
        var (projectionExpression, attributeNames) = ProjectionBuilder.Build<T>(projection);
        ProjectionExpression = projectionExpression;
        foreach (var kvp in attributeNames)
        {
            ExpressionAttributeNames[kvp.Key] = kvp.Value;
        }
    }
}
