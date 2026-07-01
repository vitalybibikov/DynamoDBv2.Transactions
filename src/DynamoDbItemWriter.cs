using System.Linq.Expressions;
using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Handles single-item, non-transactional DynamoDB writes using an abstraction over the AWS SDK.
/// Builds the same request objects as <see cref="DynamoDbTransactor"/> (the single source of truth
/// for the write semantics) and executes each immediately as a plain <c>UpdateItem</c>/<c>PutItem</c>/
/// <c>DeleteItem</c> via <see cref="ISingleWriteManager"/>.
/// </summary>
public class DynamoDbItemWriter : IDynamoDbItemWriter
{
    private readonly ISingleWriteManager _manager;

    /// <summary>
    /// Initializes a new instance of the <see cref="DynamoDbItemWriter"/> class with a specific
    /// single-write manager.
    /// </summary>
    /// <param name="manager">The manager that executes each request as a plain item write.</param>
    public DynamoDbItemWriter(ISingleWriteManager manager)
    {
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DynamoDbItemWriter"/> class with an Amazon DynamoDB client.
    /// </summary>
    /// <param name="client">The Amazon DynamoDB client used to create a single-write manager.</param>
    public DynamoDbItemWriter(IAmazonDynamoDB client)
        : this(new SingleWriteManager(client))
    {
    }

    /// <inheritdoc />
    public Task CreateOrUpdateAsync<T>(T item, CancellationToken token = default)
    {
        var request = new PutTransactionRequest<T>(item);
        return _manager.ExecuteAsync(request, token);
    }

    /// <inheritdoc />
    public Task PatchAsync<T>(T model, string propertyName, CancellationToken token = default)
    {
        var request = new PatchTransactionRequest<T>(model, propertyName);
        return _manager.ExecuteAsync(request, token);
    }

    /// <inheritdoc />
    public Task PatchAsync<T>(T model, bool incrementVersion, params string[] propertyNames)
    {
        return PatchAsync(model, incrementVersion, (IReadOnlyCollection<string>)propertyNames, default);
    }

    /// <inheritdoc />
    public Task PatchAsync<T>(T model, bool incrementVersion, IReadOnlyCollection<string> propertyNames, CancellationToken token = default)
    {
        var request = new PatchManyTransactionRequest<T>(model, propertyNames, incrementVersion);
        return _manager.ExecuteAsync(request, token);
    }

    /// <inheritdoc />
    public Task PatchAsync<TModel, TValue>(
        string keyValue,
        Expression<Func<TModel, TValue?>> propertyExpression,
        TValue value,
        CancellationToken token = default)
    {
        if (keyValue == null)
        {
            throw new ArgumentNullException(nameof(keyValue));
        }

        if (propertyExpression.Body is not MemberExpression member)
        {
            throw new ArgumentException("Expression is not a member access", nameof(propertyExpression));
        }

        var request = new PatchTransactionRequest<TModel>(keyValue, new Property
        {
            Name = member.Member.Name,
            Value = value
        });

        return _manager.ExecuteAsync(request, token);
    }

    /// <inheritdoc />
    public Task DeleteAsync<T>(string keyValue, CancellationToken token = default)
    {
        if (keyValue == null)
        {
            throw new ArgumentNullException(nameof(keyValue));
        }

        var request = new DeleteTransactionRequest<T>(keyValue);
        return _manager.ExecuteAsync(request, token);
    }

    /// <inheritdoc />
    public Task DeleteAsync<T>(string hashKeyValue, string rangeKeyValue, CancellationToken token = default)
    {
        if (hashKeyValue == null)
        {
            throw new ArgumentNullException(nameof(hashKeyValue));
        }

        if (rangeKeyValue == null)
        {
            throw new ArgumentNullException(nameof(rangeKeyValue));
        }

        var request = new DeleteTransactionRequest<T>(hashKeyValue, rangeKeyValue);
        return _manager.ExecuteAsync(request, token);
    }

    /// <inheritdoc />
    public Task WriteRawAsync(ITransactionRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return _manager.ExecuteAsync(request, token);
    }
}
