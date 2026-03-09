using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Gets or sets the compile-time generated type mapping for a DynamoDB entity.
/// Implementations are source-generated and registered automatically via module initializer.
/// </summary>
public interface IGeneratedTypeMapping
{
    /// <summary>
    /// Gets the DynamoDB table name for this type.
    /// </summary>
    string TableName { get; }

    /// <summary>
    /// Gets the DynamoDB hash key attribute name for this type.
    /// </summary>
    string HashKeyAttributeName { get; }

    /// <summary>
    /// Gets the DynamoDB attribute name for a given C# property name.
    /// </summary>
    /// <param name="propertyName">The C# property name.</param>
    /// <returns>The DynamoDB attribute name.</returns>
    string GetPropertyAttributeName(string propertyName);

    /// <summary>
    /// Gets or sets a value indicating whether to map all properties of the given object to a DynamoDB attribute dictionary.
    /// </summary>
    /// <param name="obj">The entity instance.</param>
    /// <param name="conversion">The conversion schema (V1 or V2).</param>
    /// <returns>Dictionary of attribute name to AttributeValue.</returns>
    Dictionary<string, AttributeValue> MapToAttributes(object obj, DynamoDBEntryConversion? conversion);

    /// <summary>
    /// Gets the version property name and current value from the given entity.
    /// </summary>
    /// <param name="item">The entity instance.</param>
    /// <returns>Tuple of version property name (or null) and current value.</returns>
    (string? VersionProperty, object? Value) GetVersion(object item);

    /// <summary>
    /// Gets the DynamoDB range key attribute name for this type, or null if there is no range key.
    /// </summary>
    string? RangeKeyAttributeName { get; }

    /// <summary>
    /// Deserializes a DynamoDB attribute dictionary back to a typed object instance.
    /// </summary>
    /// <param name="attributes">The DynamoDB attributes.</param>
    /// <returns>A new instance of the entity with properties populated from the attributes.</returns>
    object MapFromAttributes(Dictionary<string, AttributeValue> attributes);
}
