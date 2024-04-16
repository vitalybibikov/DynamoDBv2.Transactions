using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions.Contracts
{
    public interface ITransactionManager
    {
        Task<TransactWriteItemsResponse?> ExecuteTransactionAsync(
            IEnumerable<ITransactionRequest> requests,
            CancellationToken token = default);
    }
}
