using System.Linq.Expressions;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions.Contracts;

/// <summary>
/// Writes a single DynamoDB item WITHOUT a transaction, using a plain
/// <c>UpdateItem</c>/<c>PutItem</c>/<c>DeleteItem</c>. Each method executes and awaits immediately —
/// unlike <see cref="IDynamoDbTransactor"/>, which accumulates operations and flushes them as one
/// <c>TransactWriteItems</c> on <c>DisposeAsync</c>.
///
/// Because the write is not wrapped in a transaction it serializes at the item level and is never
/// aborted by a concurrent non-transactional writer — it throws
/// <see cref="Amazon.DynamoDBv2.Model.ConditionalCheckFailedException"/> only when its own condition
/// fails, never <c>TransactionCanceledException</c>/<c>TransactionConflictException</c> — and it costs
/// 1 WCU instead of 2. Use this for hot single-item paths; use <see cref="IDynamoDbTransactor"/> when
/// two or more items must commit atomically. Request building is shared with the transactor, so the
/// produced expressions, keys, <c>attribute_exists</c> guard and <c>ADD Version</c> bump are identical.
/// Note: <c>ADD Version</c> is not idempotent across separate successful calls (an SDK auto-retry of a
/// committed-but-unacknowledged write double-bumps); treat the version as a change/fencing token, not a
/// counted quantity.
/// </summary>
public interface IDynamoDbItemWriter
{
    /// <summary>
    /// Creates or overwrites one item via a plain <c>PutItem</c>. On a versioned type the version
    /// equality condition still applies and a mismatch surfaces as
    /// <see cref="Amazon.DynamoDBv2.Model.ConditionalCheckFailedException"/>.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="item">The item to create or overwrite.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task CreateOrUpdateAsync<T>(T item, CancellationToken token = default);

    /// <summary>
    /// Patches a single attribute of one existing item via a plain <c>UpdateItem</c>.
    /// </summary>
    /// <typeparam name="T">The model type.</typeparam>
    /// <param name="model">The model instance carrying the value to write and the key.</param>
    /// <param name="propertyName">The CLR property name of the attribute to patch.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task PatchAsync<T>(T model, string propertyName, CancellationToken token = default);

    /// <summary>
    /// Patches several attributes of one existing item in a single <c>UpdateItem</c>, optionally bumping
    /// the <c>[DynamoDBVersion]</c> attribute atomically via <c>ADD</c> (no equality condition, so it
    /// never fails on a version race). Only the listed attributes are written — the motivating
    /// conflict-free case.
    /// </summary>
    /// <typeparam name="T">The model type.</typeparam>
    /// <param name="model">The model instance carrying the attribute values to write and the key.</param>
    /// <param name="incrementVersion">Whether to atomically increment the version attribute.</param>
    /// <param name="propertyNames">The CLR property names of the attributes to patch.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task PatchAsync<T>(T model, bool incrementVersion, params string[] propertyNames);

    /// <summary>
    /// Overload of the multi-attribute patch taking an explicit property collection and a cancellation
    /// token (a token cannot follow a <c>params</c> argument).
    /// </summary>
    /// <typeparam name="T">The model type.</typeparam>
    /// <param name="model">The model instance carrying the attribute values to write and the key.</param>
    /// <param name="incrementVersion">Whether to atomically increment the version attribute.</param>
    /// <param name="propertyNames">The CLR property names of the attributes to patch.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task PatchAsync<T>(T model, bool incrementVersion, IReadOnlyCollection<string> propertyNames, CancellationToken token = default);

    /// <summary>
    /// Patches a single attribute by hash key value and a property expression via a plain <c>UpdateItem</c>.
    /// </summary>
    /// <typeparam name="TModel">The model type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="keyValue">The hash key value of the item.</param>
    /// <param name="propertyExpression">A property expression selecting the attribute to patch.</param>
    /// <param name="value">The value to set.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task PatchAsync<TModel, TValue>(string keyValue, Expression<Func<TModel, TValue?>> propertyExpression, TValue value, CancellationToken token = default);

    /// <summary>
    /// Deletes one item by its hash key value via a plain <c>DeleteItem</c>. Assumes the hash key is
    /// marked with <see cref="Amazon.DynamoDBv2.DataModel.DynamoDBHashKeyAttribute"/>.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="keyValue">The hash key value of the item to delete.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task DeleteAsync<T>(string keyValue, CancellationToken token = default);

    /// <summary>
    /// Deletes one item by its composite (hash + range) key via a plain <c>DeleteItem</c>.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="hashKeyValue">The hash key value of the item to delete.</param>
    /// <param name="rangeKeyValue">The range key value of the item to delete.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task DeleteAsync<T>(string hashKeyValue, string rangeKeyValue, CancellationToken token = default);

    /// <summary>
    /// Executes any single already-built request as a plain, non-transactional item write. Throws for a
    /// <c>ConditionCheck</c> (no standalone equivalent). The escape hatch for building requests directly.
    /// </summary>
    /// <param name="request">The single request to execute.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    Task WriteRawAsync(ITransactionRequest request, CancellationToken token = default);
}
