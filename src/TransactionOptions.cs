using Amazon.DynamoDBv2;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Options for configuring DynamoDB transaction behavior.
/// </summary>
public class TransactionOptions
{
    /// <summary>
    /// Providing a ClientRequestToken makes the call to TransactWriteItems idempotent,
    /// meaning that multiple identical calls have the same effect as one single call.
    /// A client request token is valid for 10 minutes after the first request that uses it completes.
    /// </summary>
    public string? ClientRequestToken { get; set; }

    /// <summary>
    /// Determines the level of detail about either provisioned or on-demand throughput consumption
    /// that is returned in the response.
    /// </summary>
    public ReturnConsumedCapacity? ReturnConsumedCapacity { get; set; }

    /// <summary>
    /// Determines whether item collection metrics are returned.
    /// </summary>
    public ReturnItemCollectionMetrics? ReturnItemCollectionMetrics { get; set; }
}
