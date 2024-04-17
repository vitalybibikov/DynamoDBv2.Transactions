using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions;

public static class TransactionalWriterExtensions
{
    public static void PatchAsync<T>(this TransactionalWriter writer, string keyValue, string propertyName, string propertyValue)
    {
        var request = new PatchTransactionRequest<T>(keyValue, new Property()
        {
            Name = propertyName,
            Value = propertyValue
        });

        writer.AddRawRequest(request);
    }

    public static void DeleteAsync<T>(this TransactionalWriter writer, string keyValue)
        where T : ITransactional
    {
        var request = new DeleteTransactionRequest<T>(keyValue);
        writer.AddRawRequest(request);
    }
}