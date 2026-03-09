using Amazon.DynamoDBv2;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Options for configuring DynamoDB read transaction behavior.
/// </summary>
public class ReadTransactionOptions
{
    /// <summary>
    /// Gets or sets the level of detail about provisioned or on-demand throughput consumption
    /// that is returned in the response.
    /// </summary>
    public ReturnConsumedCapacity? ReturnConsumedCapacity { get; set; }
}
