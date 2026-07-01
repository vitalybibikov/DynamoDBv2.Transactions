using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;

namespace DynamoDBv2.Transactions.Requests;

/// <summary>
/// Represents a request that patches several attributes of a single existing item in one
/// <c>UpdateItem</c> operation, optionally incrementing the model's <c>[DynamoDBVersion]</c>
/// attribute atomically.
///
/// Unlike <see cref="PatchTransactionRequest{T}"/> (single attribute) this writes only the
/// listed attributes, so concurrent writers that touch <em>different</em> attributes of the
/// same item never conflict. When <c>incrementVersion</c> is <c>true</c> the version is bumped
/// via <c>ADD</c> (never overwritten and with no equality condition), so the patch itself never
/// fails on a version race while still invalidating any concurrent full-object,
/// version-checked write — keeping such writers fail-safe rather than clobbering.
///
/// A <c>attribute_exists(hashKey)</c> condition guarantees the item already exists (a patch
/// never creates a partial item).
/// </summary>
public sealed class PatchManyTransactionRequest<T> : TransactionRequest
{
    public string? UpdateExpression { get; private set; }

    public override TransactOperationType Type => TransactOperationType.Patch;

    public PatchManyTransactionRequest(T model, IReadOnlyCollection<string> propertyNames, bool incrementVersion)
        : base(typeof(T))
    {
        ArgumentNullException.ThrowIfNull(model);

        if (propertyNames == null || propertyNames.Count == 0)
        {
            throw new ArgumentException("At least one property name must be provided.", nameof(propertyNames));
        }

        var attributes = DynamoDbMapper.MapToAttribute(model);

        Key = BuildKey(attributes);

        var setClauses = new List<string>(propertyNames.Count);
        var index = 0;

        foreach (var propertyName in propertyNames)
        {
            var attributeName = DynamoDbMapper.GetPropertyAttributedName(ItemType, propertyName);

            if (!attributes.TryGetValue(attributeName, out var attributeValue))
            {
                // MapToAttribute skips null values — patch them as an explicit NULL.
                attributeValue = new AttributeValue { NULL = true };
            }

            var nameToken = $"#p{index}";
            var valueToken = $":v{index}";

            ExpressionAttributeNames[nameToken] = attributeName;
            ExpressionAttributeValues[valueToken] = attributeValue;
            setClauses.Add($"{nameToken} = {valueToken}");

            index++;
        }

        var expression = "SET " + string.Join(", ", setClauses);

        if (incrementVersion)
        {
            var (versionAttributeName, _) = DynamoDbMapper.GetVersion(model);

            if (versionAttributeName == null)
            {
                throw new InvalidOperationException(
                    $"Type {typeof(T).Name} has no [DynamoDBVersion] property; cannot increment version.");
            }

            ExpressionAttributeNames["#version"] = versionAttributeName;
            ExpressionAttributeValues[":increment"] = new AttributeValue { N = "1" };
            expression += " ADD #version :increment";
        }

        UpdateExpression = expression;

        var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
        ExpressionAttributeNames["#hashKey"] = hashKeyName;
        ConditionExpression = "attribute_exists(#hashKey)";
    }

    public override Operation GetOperation()
    {
        var update = new Update
        {
            TableName = TableName,
            Key = Key
        };

        if (ExpressionAttributeNames.Count > 0)
        {
            update.ExpressionAttributeNames = ExpressionAttributeNames;
        }

        if (ExpressionAttributeValues.Count > 0)
        {
            update.ExpressionAttributeValues = ExpressionAttributeValues;
        }

        if (!String.IsNullOrEmpty(ConditionExpression))
        {
            update.ConditionExpression = ConditionExpression;
        }

        if (!String.IsNullOrEmpty(UpdateExpression))
        {
            update.UpdateExpression = UpdateExpression;
        }

        if (ReturnValuesOnConditionCheckFailure != null)
        {
            update.ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure;
        }

        return Operation.Patch(update);
    }

    private Dictionary<string, AttributeValue> BuildKey(IDictionary<string, AttributeValue> attributes)
    {
        var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));

        if (!attributes.TryGetValue(hashKeyName, out var hashKeyValue))
        {
            throw new ArgumentException(
                $"Hash key '{hashKeyName}' not found in mapped attributes for type {typeof(T).Name}. Ensure the hash key property is non-null.");
        }

        var rangeKeyName = DynamoDbMapper.TryGetRangeKeyAttributeName(typeof(T));

        if (rangeKeyName != null)
        {
            if (!attributes.TryGetValue(rangeKeyName, out var rangeKeyValue))
            {
                throw new ArgumentException(
                    $"Range key '{rangeKeyName}' not found in mapped attributes for type {typeof(T).Name}. Ensure the range key property is non-null.");
            }

            return new Dictionary<string, AttributeValue>
            {
                { hashKeyName, hashKeyValue },
                { rangeKeyName, rangeKeyValue }
            };
        }

        return new Dictionary<string, AttributeValue>
        {
            { hashKeyName, hashKeyValue }
        };
    }
}
