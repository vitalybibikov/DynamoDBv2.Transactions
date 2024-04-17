using System.Linq.Expressions;
using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions;

public sealed class DynamoDbTransactor : IAsyncDisposable
{
    private readonly ITransactionManager _manager;
    private bool failedToProcess = false;

    private List<ITransactionRequest> Requests { get; } = [];

    public DynamoDbTransactor(ITransactionManager manager)
    {
        _manager = manager;
    }

    public DynamoDbTransactor(IAmazonDynamoDB manager)
    : this(new TransactionManager(manager))
    {
    }

    public void CreateOrUpdate<T>(T item)
        where T : ITransactional
    {
        try
        {
            var putRequest = new PutTransactionRequest<T>(item);
            AddRawRequest(putRequest);
        }
        catch (Exception)
        {
            failedToProcess = true;
            throw;
        }
    }

    public void PatchAsync<T>(T model, string propertyName)
    {
        try
        {
            var request = new PatchTransactionRequest<T>(model, propertyName);
            AddRawRequest(request);
        }
        catch (Exception)
        {
            failedToProcess = true;
            throw;
        }
    }

    public void PatchAsync<TModel, TValue>(
        string keyValue,
        Expression<Func<TModel, TValue?>> propertyExpression,
        TValue value)
    {
        try
        {
            var member = propertyExpression.Body as MemberExpression;

            if (keyValue == null)
            {
                throw new ArgumentNullException(nameof(keyValue));
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
        catch (Exception)
        {
            failedToProcess = true;
            throw;
        }
    }

    public void DeleteAsync<T>(string key, string value)
    {
        try
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var request = new DeleteTransactionRequest<T>(new KeyValue()
            {
                Key = key,
                Value = value
            });

            AddRawRequest(request);
        }
        catch (Exception)
        {
            failedToProcess = true;
            throw;
        }
    }

    public void DeleteAsync<TModel, TKeyValue>(
        Expression<Func<TModel, string>> propertyExpression,
        string value)
    {
        try
        {
            var member = propertyExpression.Body as MemberExpression;

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (member == null)
            {
                throw new ArgumentException("Expression is not a member access", nameof(propertyExpression));
            }
            var propertyName = member.Member.Name;

            var request = new DeleteTransactionRequest<TModel>(new KeyValue()
            {
                Key = propertyName,
                Value = value
            });

            AddRawRequest(request);
        }
        catch (Exception)
        {
            failedToProcess = true;
            throw;
        }
    }

    public void AddRawRequest(ITransactionRequest request)
    {
        try
        {
            Requests.Add(request);
        }
        catch (Exception)
        {
            failedToProcess = true;
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!failedToProcess)
        {
            await _manager.ExecuteTransactionAsync(Requests);
        }
    }
}