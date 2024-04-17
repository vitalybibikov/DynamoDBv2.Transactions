using System.Linq.Expressions;
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

    public void PatchAsync<T>(T model, string propertyName)
    {
        var request = new PatchTransactionRequest<T>(model, propertyName);
        AddRawRequest(request);
    }

    public void PatchAsync<TModel, TValue>(
        string keyValue,
        Expression<Func<TModel, TValue>> propertyExpression,
        TValue value)
    {
        var member = propertyExpression.Body as MemberExpression;

        if (keyValue == null)
        {
            throw new ArgumentNullException(nameof(keyValue));
        }

        if (value == null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        if (member == null)
        {
            throw new ArgumentException("Expression is not a member access", nameof(propertyExpression));
        }

        var propertyName = member.Member.Name;

        var request = new PatchTransactionRequest<TModel>(keyValue, new Property()
        {
            Name = propertyName,
            Value = value
        });

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