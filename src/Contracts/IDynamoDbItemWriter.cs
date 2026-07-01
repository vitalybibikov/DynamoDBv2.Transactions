using System.Linq.Expressions;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions.Contracts;

/// <summary>
/// Writes a single DynamoDB item WITHOUT a transaction, using a plain
/// <c>UpdateItem</c>/<c>PutItem</c>/<c>DeleteItem</c>. Each method executes and awaits immediately —
/// unlike <see cref="IDynamoDbTransactor"/>, which accumulates operations and flushes them as one
/// <c>TransactWriteItems</c> on <c>DisposeAsync</c>.
///
/// Because the write is not wrapped in a transaction it:
/// <list type="bullet">
/// <item>serializes at the item level and is never aborted by a concurrent non-transactional writer —
/// it throws <see cref="Amazon.DynamoDBv2.Model.ConditionalCheckFailedException"/> only when its own
/// condition fails, never <c>TransactionCanceledException</c>/<c>TransactionConflictException</c>;</item>
/// <item>costs 1 WCU instead of 2.</item>
/// </list>
///
/// Use this for hot single-item paths; use <see cref="IDynamoDbTransactor"/> when two or more items
/// must commit atomically. Request building is shared with the transactor, so the produced
/// expressions, keys, <c>attribute_exists</c> guard and <c>ADD Version</c> bump are identical.
/// Note: <c>ADD Version</c> is not idempotent across separate successful calls (an SDK auto-retry of a
/// committed-but-unacknowledged write double-bumps); the version is a change/fencing token, not a
/// counted quantity, so this is acceptable — do not rely on the writer being retry-idempotent.
/// </summary>
public interface IDynamoDbItemWriter
{
    /// <summary>
    /// Creates or overwrites one item via a plain <c>PutItem</c>. On a <c>[DynamoDBVersion]</c> type the
    /// version-equality condition still applies and a mismatch surfaces as
    /// <see cref="Amazon.DynamoDBv2.Model.ConditionalCheckFailedException"/>. Mirrors
    /// <see cref="IDynamoDbTransactor.CreateOrUpdate{T}"/>.
    /// </summary>
    Task CreateOrUpdateAsync<T>(T item, CancellationToken token = default);

    /// <summary>
    /// Patches a single attribute of one existing item via a plain <c>UpdateItem</c>. Mirrors
    /// <see cref="IDynamoDbTransactor.PatchAsync{T}(T, string)"/>.
    /// </summary>
    Task PatchAsync<T>(T model, string propertyName, CancellationToken token = default);

    /// <summary>
    /// Patches several attributes of one existing item in a single <c>UpdateItem</c>, optionally bumping
    /// <c>[DynamoDBVersion]</c> atomically via <c>ADD</c> (no equality condition, so it never fails on a
    /// version race). Only the listed attributes are written — the motivating conflict-free case.
    /// Mirrors <see cref="IDynamoDbTransactor.PatchAsync{T}(T, bool, string[])"/>.
    /// </summary>
    Task PatchAsync<T>(T model, bool incrementVersion, params string[] propertyNames);

    /// <summary>
    /// Overload of the multi-attribute patch taking an explicit property collection and a
    /// <see cref="CancellationToken"/> (a token cannot follow a <c>params</c> argument).
    /// </summary>
    Task PatchAsync<T>(T model, bool incrementVersion, IReadOnlyCollection<string> propertyNames, CancellationToken token = default);

    /// <summary>
    /// Patches a single attribute by hash key value and a property expression via a plain <c>UpdateItem</c>.
    /// Mirrors <see cref="IDynamoDbTransactor.PatchAsync{TModel, TValue}(string, Expression{Func{TModel, TValue}}, TValue)"/>.
    /// </summary>
    Task PatchAsync<TModel, TValue>(string keyValue, Expression<Func<TModel, TValue?>> propertyExpression, TValue value, CancellationToken token = default);

    /// <summary>
    /// Deletes one item by its hash key value via a plain <c>DeleteItem</c>. Assumes the hash key is
    /// marked with <see cref="Amazon.DynamoDBv2.DataModel.DynamoDBHashKeyAttribute"/>.
    /// </summary>
    Task DeleteAsync<T>(string keyValue, CancellationToken token = default);

    /// <summary>
    /// Deletes one item by its composite (hash + range) key via a plain <c>DeleteItem</c>.
    /// </summary>
    Task DeleteAsync<T>(string hashKeyValue, string rangeKeyValue, CancellationToken token = default);

    /// <summary>
    /// Executes any single already-built request as a plain, non-transactional item write. Throws for
    /// a <c>ConditionCheck</c> (no standalone equivalent). The escape hatch symmetric with
    /// <see cref="IDynamoDbTransactor.AddRawRequest"/>.
    /// </summary>
    Task WriteRawAsync(ITransactionRequest request, CancellationToken token = default);
}
