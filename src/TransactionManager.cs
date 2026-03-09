using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Operates on multiple DynamoDB tables to store data in a single transaction.
/// </summary>
/// <param name="client"><see cref="IAmazonDynamoDB"/>.</param>
public sealed class TransactionManager(IAmazonDynamoDB client)
    : ITransactionManager
{
    /// <summary>
    /// DynamoDB TransactWriteItems API limit.
    /// </summary>
    internal const int MaxTransactionItems = 100;

    /// <summary>
    /// In transaction, asynchronously saves items to 1 or many DynamoDB tables.
    /// </summary>
    /// <param name="requests">Lists of the operations to be performed during a transaction. </param>
    /// <param name="token">Cancellation token. </param>
    /// <returns cref="TransactWriteItemsResponse">Returns TransactWriteItemsResponse response. </returns>
    /// <exception cref="ArgumentOutOfRangeException">Might throw ArgumentOutOfRangeException. </exception>
    public Task<TransactWriteItemsResponse?> ExecuteTransactionAsync(
        IEnumerable<ITransactionRequest> requests,
        CancellationToken token = default)
    {
        return ExecuteTransactionAsync(requests, options: null, token);
    }

    /// <summary>
    /// In transaction, asynchronously saves items to 1 or many DynamoDB tables with options.
    /// </summary>
    /// <param name="requests">Lists of the operations to be performed during a transaction. </param>
    /// <param name="options">Transaction options (idempotency token, consumed capacity, etc.). </param>
    /// <param name="token">Cancellation token. </param>
    /// <returns cref="TransactWriteItemsResponse">Returns TransactWriteItemsResponse response. </returns>
    /// <exception cref="ArgumentOutOfRangeException">Might throw ArgumentOutOfRangeException. </exception>
    /// <exception cref="ArgumentException">Thrown when more than 100 items are in the transaction. </exception>
    public async Task<TransactWriteItemsResponse?> ExecuteTransactionAsync(
        IEnumerable<ITransactionRequest> requests,
        TransactionOptions? options,
        CancellationToken token = default)
    {
        int initialCapacity = requests is ICollection<ITransactionRequest> col ? col.Count : 0;
#if NET6_0_OR_GREATER
        if (initialCapacity == 0)
        {
            requests.TryGetNonEnumeratedCount(out initialCapacity);
        }
#endif
        var transactWriteItems = new List<TransactWriteItem>(initialCapacity);

        foreach (var transactionRequest in requests)
        {
            var request = (TransactionRequest)transactionRequest;
            var item = new TransactWriteItem();

            switch (request.Type)
            {
                case TransactOperationType.Put:
                    item.Put = request.GetOperation().PutType;
                    break;

                case TransactOperationType.Update:
                    item.Update = request.GetOperation().UpdateType;
                    break;

                case TransactOperationType.Delete:
                    item.Delete = request.GetOperation().DeleteType;
                    break;

                case TransactOperationType.ConditionCheck:
                    item.ConditionCheck = request.GetOperation().ConditionCheckType;
                    break;

                case TransactOperationType.Patch:
                    item.Update = request.GetOperation().UpdateType;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(request), $"Unsupported transaction request type: {request.GetType().Name}");
            }

            transactWriteItems.Add(item);
        }

        if (transactWriteItems.Count > MaxTransactionItems)
        {
            throw new ArgumentException(
                $"DynamoDB transactions support a maximum of {MaxTransactionItems} items, but {transactWriteItems.Count} were provided.",
                nameof(requests));
        }

        var transactionWriteRequest = new TransactWriteItemsRequest { TransactItems = transactWriteItems };

        if (options != null)
        {
            if (!string.IsNullOrEmpty(options.ClientRequestToken))
            {
                transactionWriteRequest.ClientRequestToken = options.ClientRequestToken;
            }

            if (options.ReturnConsumedCapacity != null)
            {
                transactionWriteRequest.ReturnConsumedCapacity = options.ReturnConsumedCapacity;
            }

            if (options.ReturnItemCollectionMetrics != null)
            {
                transactionWriteRequest.ReturnItemCollectionMetrics = options.ReturnItemCollectionMetrics;
            }
        }

        var response = await client.TransactWriteItemsAsync(transactionWriteRequest, token);

        return response;
    }
}
