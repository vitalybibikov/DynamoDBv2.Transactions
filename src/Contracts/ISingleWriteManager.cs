using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions.Contracts;

/// <summary>
/// Executes a single <see cref="ITransactionRequest"/> as a plain, non-transactional item write
/// (<c>UpdateItem</c>/<c>PutItem</c>/<c>DeleteItem</c>).
///
/// This is the write-side, single-item counterpart to <see cref="ITransactionManager"/>: it owns the
/// <c>Operation → *ItemRequest</c> mapping, exactly as <see cref="ITransactionManager"/> owns the
/// <c>Operation → TransactWriteItem</c> mapping. Because the write is not wrapped in
/// <c>TransactWriteItems</c>, it serializes at the item level (never aborted by a concurrent writer),
/// costs 1 WCU instead of 2, and surfaces a failed condition as
/// <see cref="Amazon.DynamoDBv2.Model.ConditionalCheckFailedException"/> rather than
/// <c>TransactionCanceledException</c>.
/// </summary>
public interface ISingleWriteManager
{
    /// <summary>
    /// Executes one request as a plain, non-transactional item write.
    /// </summary>
    /// <param name="request">The single request to execute. Must be a Put/Update/Patch/Delete request.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="request"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the request does not inherit <c>TransactionRequest</c> or is a
    /// <c>ConditionCheck</c> (which has no standalone, non-transactional equivalent).
    /// </exception>
    Task ExecuteAsync(ITransactionRequest request, CancellationToken token = default);
}
