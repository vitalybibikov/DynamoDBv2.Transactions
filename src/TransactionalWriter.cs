using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions;

public sealed class TransactionalWriter : IAsyncDisposable
{
    private readonly ITransactionManager _manager;

    private List<ITransactionRequest> Requests { get; } = [];

    public TransactionalWriter(ITransactionManager manager)
    {
        _manager = manager;
    }

    public void CreateOrUpdate<T>(T item)
        where T : ITransactional
    {
        var putRequest = new PutTransactionRequest<T>(item);

        AddRawRequest(putRequest);
    }

    public void PatchAsync<T, TV>(KeyValue key, Property value)
    {
        var request = new PatchTransactionRequest<T>(key, value!);
        AddRawRequest(request);
    }

    public void PatchAsync<T, TV>(string keyName, Property value)
    {
        var request = new PatchTransactionRequest<T>(keyName, value!);
        AddRawRequest(request);
    }

    public void DeleteAsync<T>(KeyValue key)
        where T : ITransactional
    {
        var request = new DeleteTransactionRequest<T>(key);
        AddRawRequest(request);
    }

    public void AddRawRequest(ITransactionRequest request)
    {
        Requests.Add(request);
    }

    public async ValueTask DisposeAsync()
    {
        await _manager.ExecuteTransactionAsync(Requests);
    }
}