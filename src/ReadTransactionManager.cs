using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Executes DynamoDB transactional get operations against one or many tables.
/// </summary>
/// <param name="client"><see cref="IAmazonDynamoDB"/>.</param>
public sealed class ReadTransactionManager(IAmazonDynamoDB client)
    : IReadTransactionManager
{
    /// <summary>
    /// DynamoDB TransactGetItems API limit.
    /// </summary>
    internal const int MaxTransactionItems = 100;

    /// <summary>
    /// Executes a transactional get operation.
    /// </summary>
    /// <param name="requests">The get requests to execute.</param>
    /// <param name="options">Optional read transaction options.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>The raw <see cref="TransactGetItemsResponse"/>.</returns>
    public async Task<TransactGetItemsResponse?> ExecuteGetTransactionAsync(
        IEnumerable<IGetTransactionRequest> requests,
        ReadTransactionOptions? options = null,
        CancellationToken token = default)
    {
        int initialCapacity = requests is ICollection<IGetTransactionRequest> col ? col.Count : 0;
#if NET6_0_OR_GREATER
        if (initialCapacity == 0)
        {
            requests.TryGetNonEnumeratedCount(out initialCapacity);
        }
#endif
        var transactGetItems = new List<TransactGetItem>(initialCapacity);

        foreach (var request in requests)
        {
            var get = new Get
            {
                TableName = request.TableName,
                Key = request.Key
            };

            if (!string.IsNullOrEmpty(request.ProjectionExpression))
            {
                get.ProjectionExpression = request.ProjectionExpression;
            }

            if (request.ExpressionAttributeNames.Count > 0)
            {
                get.ExpressionAttributeNames = request.ExpressionAttributeNames;
            }

            transactGetItems.Add(new TransactGetItem { Get = get });
        }

        if (transactGetItems.Count == 0)
        {
            throw new ArgumentException(
                "DynamoDB TransactGetItems requires at least one item.",
                nameof(requests));
        }

        if (transactGetItems.Count > MaxTransactionItems)
        {
            throw new ArgumentException(
                $"DynamoDB transactions support a maximum of {MaxTransactionItems} items, but {transactGetItems.Count} were provided.",
                nameof(requests));
        }

        var transactGetRequest = new TransactGetItemsRequest
        {
            TransactItems = transactGetItems
        };

        if (options?.ReturnConsumedCapacity != null)
        {
            transactGetRequest.ReturnConsumedCapacity = options.ReturnConsumedCapacity;
        }

        var response = await client.TransactGetItemsAsync(transactGetRequest, token);

        return response;
    }
}
