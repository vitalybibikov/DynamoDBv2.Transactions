using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions;

public static class TransactionalWriterExtensions
{
    public static void PatchAsync<T, TV>(this TransactionalWriter writer, string keyValue, Property value)
    {
        var request = new PatchTransactionRequest<T>(keyValue, value!);
        writer.AddRawRequest(request);
    }

    public static void DeleteAsync<T>(this TransactionalWriter writer, string keyValue)
        where T : ITransactional
    {
        var request = new DeleteTransactionRequest<T>(keyValue);
        writer.AddRawRequest(request);
    }
}