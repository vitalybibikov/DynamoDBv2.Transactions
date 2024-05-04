using System.Linq.Expressions;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Handles DynamoDB transaction operations using an abstraction over the AWS SDK.
/// </summary>
public class DynamoDbTransactor : IDynamoDbTransactor
{
    private readonly ITransactionManager _manager;

    public bool ErrorDuringExecution { get; private set; } = false;

    private List<ITransactionRequest> Requests { get; } = [];

    /// <summary>
    /// Initializes a new instance of the DynamoDbTransactor class with a specific transaction manager.
    /// </summary>
    /// <param name="manager">The transaction manager responsible for handling the underlying database operations.</param>
    public DynamoDbTransactor(ITransactionManager manager)
    {
        _manager = manager;
    }

    /// <summary>
    /// Initializes a new instance of the DynamoDbTransactor class with an Amazon DynamoDB client.
    /// </summary>
    /// <param name="manager">The Amazon DynamoDB client used to create a transaction manager.</param>
    public DynamoDbTransactor(IAmazonDynamoDB manager)
        : this(new TransactionManager(manager))
    {
    }

    /// <summary>
    /// Adds a create or update operation to the transaction.
    /// </summary>
    /// <param name="item">The item to be created or updated in DynamoDB.</param>
    /// <typeparam name="T">The type of the item.</typeparam>
    public void CreateOrUpdate<T>(T item)
    {
        try
        {
            var putRequest = new PutTransactionRequest<T>(item);
            AddRawRequest(putRequest);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Asynchronously adds a patch operation to the transaction for a specified property of a model.
    /// </summary>
    /// <param name="model">The model instance to patch.</param>
    /// <param name="propertyName">The name of the property to patch.</param>
    /// <typeparam name="T">The type of the model.</typeparam>
    public void PatchAsync<T>(T model, string propertyName)
    {
        try
        {
            var request = new PatchTransactionRequest<T>(model, propertyName);
            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Asynchronously adds a patch operation to the transaction for a specified property using a property expression.
    /// </summary>
    /// <param name="keyValue">The key value of the item to patch.</param>
    /// <param name="propertyExpression">An expression indicating the property to patch.</param>
    /// <param name="value">The new value to assign to the property.</param>
    /// <typeparam name="TModel">The type of the model being patched.</typeparam>
    /// <typeparam name="TValue">The type of the value being patched.</typeparam>
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
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Asynchronously adds a delete operation to the transaction.
    /// </summary>
    /// <param name="key">The key of the item to delete.</param>
    /// <param name="keyValue">The value to mark the item as deleted.</param>
    /// <typeparam name="T">The type of the item.</typeparam>
    public void DeleteAsync<T>(string key, string keyValue)
    {
        try
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (keyValue == null)
            {
                throw new ArgumentNullException(nameof(keyValue));
            }

            var request = new DeleteTransactionRequest<T>(new KeyValue
            {
                Key = key,
                Value = keyValue
            });

            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Asynchronously adds a delete operation to the transaction.
    /// </summary>
    /// <param name="keyValue">The key value of the item to delete, assumes that <see cref="DynamoDBHashKeyAttribute"/> is set</param>
    /// <typeparam name="T">The type of the item.</typeparam>
    public void DeleteAsync<T>(string keyValue)
    {
        try
        {
            if (keyValue == null)
            {
                throw new ArgumentNullException(nameof(keyValue));
            }

            var request = new DeleteTransactionRequest<T>(keyValue);
            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Asynchronously adds a delete operation to the transaction using a property expression.
    /// </summary>
    /// <param name="propertyNameExpression">An expression indicating the property to use as a key for deletion.</param>
    /// <param name="keyValue">The value to mark the item as deleted.</param>
    /// <typeparam name="TModel">The type of the model.</typeparam>
    /// <typeparam name="TKeyValue">The type of the key value.</typeparam>
    public void DeleteAsync<TModel, TKeyValue>(Expression<Func<TModel, string>> propertyNameExpression, string keyValue)
    {
        try
        {
            var member = propertyNameExpression.Body as MemberExpression;

            if (keyValue == null)
            {
                throw new ArgumentNullException(nameof(keyValue));
            }

            if (member == null)
            {
                throw new ArgumentException("Expression is not a member access", nameof(propertyNameExpression));
            }

            var propertyName = member.Member.Name;

            var request = new DeleteTransactionRequest<TModel>(new KeyValue
            {
                Key = propertyName,
                Value = keyValue
            });

            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Adds various conditional operations to the transaction based on the expression and value.
    /// Includes conditions for equality, less than, greater than, and not equal.
    /// </summary>
    /// <param name="keyValue">The key and value indicating the item and property for the condition.</param>
    /// <param name="propertyExpression">An expression indicating the property to apply the condition to.</param>
    /// <param name="value">The value to compare in the condition.</param>
    /// <typeparam name="TModel">The type of the model.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <remarks>
    /// This set of methods is meant to support a variety of conditional operations for transactional integrity.
    /// </remarks>
    public void ConditionEquals<TModel, TValue>(KeyValue keyValue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyValue);
            request.Equals(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Adds a condition to the transaction ensuring the specified property value is less than the provided value.
    /// </summary>
    /// <param name="keyValue">The key and value indicating the item and property for the condition.</param>
    /// <param name="propertyExpression">An expression indicating the property to apply the condition to.</param>
    /// <param name="value">The value to compare in the condition.</param>
    /// <typeparam name="TModel">The type of the model.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public void ConditionLessThan<TModel, TValue>(KeyValue keyValue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyValue);
            request.LessThan(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Adds a condition to the transaction ensuring the specified property value is greater than the provided value.
    /// </summary>
    /// <param name="keyValue">The key and value indicating the item and property for the condition.</param>
    /// <param name="propertyExpression">An expression indicating the property to apply the condition to.</param>
    /// <param name="value">The value to compare in the condition.</param>
    /// <typeparam name="TModel">The type of the model.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public void ConditionGreaterThan<TModel, TValue>(KeyValue keyValue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyValue);
            request.GreaterThan(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Adds a condition to the transaction ensuring the specified property value is not equal to the provided value.
    /// </summary>
    /// <param name="keyValue">The key and value indicating the item and property for the condition.</param>
    /// <param name="propertyExpression">An expression indicating the property to apply the condition to.</param>
    /// <param name="value">The value to compare in the condition.</param>
    /// <typeparam name="TModel">The type of the model.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public void ConditionNotEquals<TModel, TValue>(KeyValue keyValue, Expression<Func<TModel, TValue>> propertyExpression, TValue value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyValue);
            request.NotEquals(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Adds a version-check condition to the transaction ensuring the specified property value matches the provided version.
    /// </summary>
    /// <param name="keyValue">The key and value indicating the item and property for the condition.</param>
    /// <param name="propertyExpression">An expression indicating the property to apply the version check to.</param>
    /// <param name="value">The expected version value to check against.</param>
    /// <typeparam name="TModel">The type of the model.</typeparam>
    public void ConditionVersionEquals<TModel>(KeyValue keyValue, Expression<Func<TModel, long?>> propertyExpression, long? value)
    {
        try
        {
            var request = new ConditionCheckTransactionRequest<TModel>(keyValue);
            request.VersionEquals(propertyExpression, value);

            AddRawRequest(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Adds a raw request to the list of transaction requests.
    /// </summary>
    /// <param name="request">The transaction request to add.</param>
    public virtual void AddRawRequest(ITransactionRequest request)
    {
        try
        {
            Requests.Add(request);
        }
        catch (Exception)
        {
            ErrorDuringExecution = true;
            throw;
        }
    }

    /// <summary>
    /// Asynchronously completes the transaction, executing all accumulated requests.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (!ErrorDuringExecution)
        {
            await _manager.ExecuteTransactionAsync(Requests);
        }
    }
}