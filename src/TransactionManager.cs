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
    /// In transaction, asynchronously saves items to 1 or many DynamoDB tables.
    /// </summary>
    /// <param name="requests">Lists of the operations to be performed during a transaction. </param>
    /// <param name="token">Cancellation token. </param>
    /// <returns cref="TransactWriteItemsResponse">Returns TransactWriteItemsResponse response. </returns>
    /// <exception cref="ArgumentOutOfRangeException">Might throw ArgumentOutOfRangeException. </exception>
    public async Task<TransactWriteItemsResponse?> ExecuteTransactionAsync(IEnumerable<ITransactionRequest> requests, CancellationToken token = default)
    {
        var transactWriteItems = new List<TransactWriteItem>();

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

        var transactionWriteRequest = new TransactWriteItemsRequest { TransactItems = transactWriteItems };
        var response = await client.TransactWriteItemsAsync(transactionWriteRequest, token);

        return response;
    }
}