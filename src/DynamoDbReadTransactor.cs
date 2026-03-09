using System.Linq.Expressions;
using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Handles DynamoDB transactional read operations using an abstraction over the AWS SDK.
/// Collects get requests and executes them as a single DynamoDB TransactGetItems call.
/// </summary>
public class DynamoDbReadTransactor : IDynamoDbReadTransactor, IAsyncDisposable
{
    private readonly IReadTransactionManager _manager;
    private readonly List<IGetTransactionRequest> _requests = [];

    /// <summary>
    /// Gets or sets the read transaction options.
    /// </summary>
    public ReadTransactionOptions? Options { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="DynamoDbReadTransactor"/> class with a specific read transaction manager.
    /// </summary>
    /// <param name="manager">The read transaction manager.</param>
    public DynamoDbReadTransactor(IReadTransactionManager manager)
    {
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DynamoDbReadTransactor"/> class with an Amazon DynamoDB client.
    /// </summary>
    /// <param name="client">The Amazon DynamoDB client.</param>
    public DynamoDbReadTransactor(IAmazonDynamoDB client)
        : this(new ReadTransactionManager(client))
    {
    }

    /// <inheritdoc />
    public void Get<T>(string hashKeyValue)
        where T : class, new()
    {
        var request = new GetTransactionRequest<T>(hashKeyValue);
        _requests.Add(request);
    }

    /// <inheritdoc />
    public void Get<T>(string hashKeyValue, string rangeKeyValue)
        where T : class, new()
    {
        var request = new GetTransactionRequest<T>(hashKeyValue, rangeKeyValue);
        _requests.Add(request);
    }

    /// <inheritdoc />
    public void Get<T>(string hashKeyValue, Expression<Func<T, object>> projection)
        where T : class, new()
    {
        var request = new GetTransactionRequest<T>(hashKeyValue, projection);
        _requests.Add(request);
    }

    /// <inheritdoc />
    public void Get<T>(string hashKeyValue, string rangeKeyValue, Expression<Func<T, object>> projection)
        where T : class, new()
    {
        var request = new GetTransactionRequest<T>(hashKeyValue, rangeKeyValue, projection);
        _requests.Add(request);
    }

    /// <inheritdoc />
    public async Task<TransactionGetResult> ExecuteAsync(CancellationToken token = default)
    {
        var response = await _manager.ExecuteGetTransactionAsync(_requests, Options, token);

        var items = new List<TransactionGetResult.TransactionGetResultItem>(_requests.Count);

        for (var i = 0; i < _requests.Count; i++)
        {
            var attrs = response?.Responses != null && i < response.Responses.Count
                ? response.Responses[i].Item
                : null;

            items.Add(new TransactionGetResult.TransactionGetResultItem
            {
                RequestedType = _requests[i].ItemType,
                Attributes = attrs
            });
        }

        return new TransactionGetResult(items, response?.ConsumedCapacity);
    }

    /// <summary>
    /// Disposes the transactor.
    /// </summary>
    /// <returns>A completed value task.</returns>
    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
