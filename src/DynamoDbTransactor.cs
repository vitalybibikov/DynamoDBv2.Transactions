using System.Linq.Expressions;
using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions;

public sealed class DynamoDbTransactor : IAsyncDisposable, IDynamoDbTransactor
{
    private readonly ITransactionManager _manager;
    private bool _failedToProcess = false;

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
    {
        try
        {
            var putRequest = new PutTransactionRequest<T>(item);
            AddRawRequest(putRequest);
        }
        catch (Exception)
        {
            _failedToProcess = true;
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
            _failedToProcess = true;
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
            _failedToProcess = true;
            throw;
        }
    }

    public void DeleteAsync<T>(string key, string deletedItemValue)
    {
        try
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (deletedItemValue == null)
            {
                throw new ArgumentNullException(nameof(deletedItemValue));
            }

            var request = new DeleteTransactionRequest<T>(new KeyValue()
            {
                Key = key,
                Value = deletedItemValue
            });

            AddRawRequest(request);
        }
        catch (Exception)
        {
            _failedToProcess = true;
            throw;
        }
    }

    public void DeleteAsync<TModel, TKeyValue>(Expression<Func<TModel, string>> propertyNameExpression, string deletedItemValue)
    {
        try
        {
            var member = propertyNameExpression.Body as MemberExpression;

            if (deletedItemValue == null)
            {
                throw new ArgumentNullException(nameof(deletedItemValue));
            }

            if (member == null)
            {
                throw new ArgumentException("Expression is not a member access", nameof(propertyNameExpression));
            }

            var propertyName = member.Member.Name;

            var request = new DeleteTransactionRequest<TModel>(new KeyValue
            {
                Key = propertyName,
                Value = deletedItemValue
            });

            AddRawRequest(request);
        }
        catch (Exception)
        {
            _failedToProcess = true;
            throw;
        }
    }

    public void ConditionEquals<TModel, TValue>(KeyValue keyvalue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyvalue);
            request.Equals(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception e)
        {
            _failedToProcess = true;
            throw;
        }
    }

    public void ConditionLessThan<TModel, TValue>(KeyValue keyvalue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyvalue);
            request.LessThan(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception e)
        {
            _failedToProcess = true;
            throw;
        }
    }

    public void ConditionGreaterThan<TModel, TValue>(KeyValue keyvalue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyvalue);
            request.GreaterThan(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception e)
        {
            _failedToProcess = true;
            throw;
        }
    }

    public void ConditionNotEquals<TModel, TValue>(KeyValue keyvalue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyvalue);
            request.NotEquals(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception e)
        {
            _failedToProcess = true;
            throw;
        }
    }

    public void ConditionVersionEquals<TModel>(KeyValue keyvalue, Expression<Func<TModel, long>> propertyExpression, long value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyvalue);
            request.VersionEquals(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception e)
        {
            _failedToProcess = true;
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
            _failedToProcess = true;
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_failedToProcess)
        {
            await _manager.ExecuteTransactionAsync(Requests);
        }
    }

}