using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Contains the typed results from a DynamoDB transactional get operation.
/// Items are ordered by the order they were requested.
/// </summary>
public class TransactionGetResult
{
    private readonly List<TransactionGetResultItem> _items;

    internal TransactionGetResult(
        List<TransactionGetResultItem> items,
        List<ConsumedCapacity>? consumedCapacity)
    {
        _items = items ?? throw new ArgumentNullException(nameof(items));
        ConsumedCapacity = consumedCapacity;
    }

    /// <summary>
    /// Gets the number of items in the result.
    /// </summary>
    public int Count => _items.Count;

    /// <summary>
    /// Gets the consumed capacity information, if it was requested.
    /// </summary>
    public List<ConsumedCapacity>? ConsumedCapacity { get; }

    /// <summary>
    /// Gets a typed item at the specified index.
    /// Returns null if the item was not found in DynamoDB.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="index">Zero-based index matching the order items were added to the transaction.</param>
    /// <returns>The deserialized entity or null.</returns>
    public T? GetItem<T>(int index)
        where T : class
    {
        if (index < 0 || index >= _items.Count)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index),
                $"Index {index} is out of range. Result contains {_items.Count} items.");
        }

        var item = _items[index];

        if (item.Attributes == null || item.Attributes.Count == 0)
        {
            return null;
        }

        return (T)DynamoDbMapper.MapFromAttributes(typeof(T), item.Attributes);
    }

    /// <summary>
    /// Gets the raw DynamoDB attributes at the specified index.
    /// Returns null if the item was not found.
    /// </summary>
    /// <param name="index">Zero-based index.</param>
    /// <returns>The raw attribute dictionary or null.</returns>
    public Dictionary<string, AttributeValue>? GetRawItem(int index)
    {
        if (index < 0 || index >= _items.Count)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index),
                $"Index {index} is out of range. Result contains {_items.Count} items.");
        }

        return _items[index].Attributes;
    }

    /// <summary>
    /// Gets all items of a specific type from the result.
    /// Only returns items whose request type matches <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <returns>List of deserialized entities.</returns>
    public IReadOnlyList<T> GetItems<T>()
        where T : class
    {
        var result = new List<T>();

        foreach (var item in _items)
        {
            if (item.RequestedType == typeof(T) && item.Attributes != null && item.Attributes.Count > 0)
            {
                result.Add((T)DynamoDbMapper.MapFromAttributes(typeof(T), item.Attributes));
            }
        }

        return result;
    }

    internal sealed class TransactionGetResultItem
    {
        public Type RequestedType { get; init; } = null!;
        public Dictionary<string, AttributeValue>? Attributes { get; init; }
    }
}
