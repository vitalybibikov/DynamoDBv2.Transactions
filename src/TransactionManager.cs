using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions;

public sealed class TransactionManager(IAmazonDynamoDB client)
    : ITransactionManager
{
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