using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Contract;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Executes a single <see cref="ITransactionRequest"/> as a plain, non-transactional item write.
/// The write-side, single-item counterpart to <see cref="TransactionManager"/>: it reuses the
/// request's own <see cref="TransactionRequest.GetOperation"/> (the single source of truth for the
/// expression/key/version building) and maps the resulting <c>Operation</c> onto the standalone
/// <c>UpdateItem</c>/<c>PutItem</c>/<c>DeleteItem</c> API instead of onto a <c>TransactWriteItem</c>.
/// </summary>
/// <param name="client"><see cref="IAmazonDynamoDB"/>.</param>
public sealed class SingleWriteManager(IAmazonDynamoDB client)
    : ISingleWriteManager
{
    /// <inheritdoc />
    public Task ExecuteAsync(ITransactionRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request is not TransactionRequest concrete)
        {
            throw new ArgumentException(
                $"Request must inherit from {nameof(TransactionRequest)}. Got: {request.GetType().FullName}",
                nameof(request));
        }

        var operation = concrete.GetOperation();

        return operation.Type switch
        {
            TransactOperationType.Put => client.PutItemAsync(ToPutItemRequest(operation.PutType!), token),
            TransactOperationType.Update => client.UpdateItemAsync(ToUpdateItemRequest(operation.UpdateType!), token),
            TransactOperationType.Patch => client.UpdateItemAsync(ToUpdateItemRequest(operation.UpdateType!), token),
            TransactOperationType.Delete => client.DeleteItemAsync(ToDeleteItemRequest(operation.DeleteType!), token),
            TransactOperationType.ConditionCheck => throw new ArgumentException(
                "A bare ConditionCheck has no non-transactional equivalent; attach the condition to the write instead.",
                nameof(request)),
            _ => throw new ArgumentOutOfRangeException(
                nameof(request), $"Unsupported request type: {request.GetType().Name}"),
        };
    }

    private static UpdateItemRequest ToUpdateItemRequest(Update update)
    {
        var result = new UpdateItemRequest
        {
            TableName = update.TableName,
            Key = update.Key,
            UpdateExpression = update.UpdateExpression,
        };

        if (!string.IsNullOrEmpty(update.ConditionExpression))
        {
            result.ConditionExpression = update.ConditionExpression;
        }

        if (update.ExpressionAttributeNames is { Count: > 0 })
        {
            result.ExpressionAttributeNames = update.ExpressionAttributeNames;
        }

        if (update.ExpressionAttributeValues is { Count: > 0 })
        {
            result.ExpressionAttributeValues = update.ExpressionAttributeValues;
        }

        if (update.ReturnValuesOnConditionCheckFailure != null)
        {
            result.ReturnValuesOnConditionCheckFailure = update.ReturnValuesOnConditionCheckFailure;
        }

        return result;
    }

    private static PutItemRequest ToPutItemRequest(Put put)
    {
        var result = new PutItemRequest
        {
            TableName = put.TableName,
            Item = put.Item,
        };

        if (!string.IsNullOrEmpty(put.ConditionExpression))
        {
            result.ConditionExpression = put.ConditionExpression;
        }

        if (put.ExpressionAttributeNames is { Count: > 0 })
        {
            result.ExpressionAttributeNames = put.ExpressionAttributeNames;
        }

        if (put.ExpressionAttributeValues is { Count: > 0 })
        {
            result.ExpressionAttributeValues = put.ExpressionAttributeValues;
        }

        if (put.ReturnValuesOnConditionCheckFailure != null)
        {
            result.ReturnValuesOnConditionCheckFailure = put.ReturnValuesOnConditionCheckFailure;
        }

        return result;
    }

    private static DeleteItemRequest ToDeleteItemRequest(Delete delete)
    {
        var result = new DeleteItemRequest
        {
            TableName = delete.TableName,
            Key = delete.Key,
        };

        if (!string.IsNullOrEmpty(delete.ConditionExpression))
        {
            result.ConditionExpression = delete.ConditionExpression;
        }

        if (delete.ExpressionAttributeNames is { Count: > 0 })
        {
            result.ExpressionAttributeNames = delete.ExpressionAttributeNames;
        }

        if (delete.ExpressionAttributeValues is { Count: > 0 })
        {
            result.ExpressionAttributeValues = delete.ExpressionAttributeValues;
        }

        if (delete.ReturnValuesOnConditionCheckFailure != null)
        {
            result.ReturnValuesOnConditionCheckFailure = delete.ReturnValuesOnConditionCheckFailure;
        }

        return result;
    }
}
