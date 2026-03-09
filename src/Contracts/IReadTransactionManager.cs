using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions.Contracts;

/// <summary>
/// Executes DynamoDB transactional get operations.
/// </summary>
public interface IReadTransactionManager
{
    Task<TransactGetItemsResponse?> ExecuteGetTransactionAsync(
        IEnumerable<IGetTransactionRequest> requests,
        ReadTransactionOptions? options = null,
        CancellationToken token = default);
}
