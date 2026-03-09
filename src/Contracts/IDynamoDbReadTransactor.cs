using System.Linq.Expressions;

namespace DynamoDBv2.Transactions.Contracts;

/// <summary>
/// Main interface for DynamoDB transactional read operations.
/// </summary>
public interface IDynamoDbReadTransactor
{
    /// <summary>
    /// Gets or sets the read transaction options (consumed capacity, etc.).
    /// </summary>
    ReadTransactionOptions? Options { get; set; }

    /// <summary>
    /// Adds a get operation to retrieve a full item by its hash key value.
    /// Assumes that the hash key is marked with <see cref="Amazon.DynamoDBv2.DataModel.DynamoDBHashKeyAttribute"/>.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="hashKeyValue">The hash key value.</param>
    void Get<T>(string hashKeyValue)
        where T : class, new();

    /// <summary>
    /// Adds a get operation to retrieve a full item by its composite key (hash + range).
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="hashKeyValue">The hash key value.</param>
    /// <param name="rangeKeyValue">The range key value.</param>
    void Get<T>(string hashKeyValue, string rangeKeyValue)
        where T : class, new();

    /// <summary>
    /// Adds a get operation with a projection expression to retrieve specific properties.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="hashKeyValue">The hash key value.</param>
    /// <param name="projection">A lambda selecting the properties to retrieve.</param>
    void Get<T>(string hashKeyValue, Expression<Func<T, object>> projection)
        where T : class, new();

    /// <summary>
    /// Adds a get operation with composite key and projection expression.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="hashKeyValue">The hash key value.</param>
    /// <param name="rangeKeyValue">The range key value.</param>
    /// <param name="projection">A lambda selecting the properties to retrieve.</param>
    void Get<T>(string hashKeyValue, string rangeKeyValue, Expression<Func<T, object>> projection)
        where T : class, new();

    /// <summary>
    /// Executes all queued get operations as a single DynamoDB transaction and returns typed results.
    /// </summary>
    /// <param name="token">Cancellation token.</param>
    /// <returns>The transaction result containing deserialized items.</returns>
    Task<TransactionGetResult> ExecuteAsync(CancellationToken token = default);
}
