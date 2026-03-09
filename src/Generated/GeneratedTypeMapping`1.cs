using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions;

/// <summary>
/// Wraps source-generated static methods into the <see cref="IGeneratedTypeMapping"/> interface
/// using strongly-typed delegates. One instance is created per mapped type and registered
/// via the generated [ModuleInitializer].
/// </summary>
/// <typeparam name="T">The DynamoDB entity type.</typeparam>
public sealed class GeneratedTypeMapping<T> : IGeneratedTypeMapping
    where T : class
{
    private readonly Func<string, string> _getPropertyAttributeName;
    private readonly Func<T, Dictionary<string, AttributeValue>> _mapToAttributes;
    private readonly Func<T, (string? VersionProperty, object? Value)> _getVersion;
    private readonly Func<Dictionary<string, AttributeValue>, T>? _mapFromAttributes;

    public GeneratedTypeMapping(
        string tableName,
        string hashKeyAttributeName,
        Func<string, string> getPropertyAttributeName,
        Func<T, Dictionary<string, AttributeValue>> mapToAttributes,
        Func<T, (string? VersionProperty, object? Value)> getVersion,
        string? rangeKeyAttributeName = null,
        Func<Dictionary<string, AttributeValue>, T>? mapFromAttributes = null)
    {
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        HashKeyAttributeName = hashKeyAttributeName ?? throw new ArgumentNullException(nameof(hashKeyAttributeName));
        _getPropertyAttributeName = getPropertyAttributeName ?? throw new ArgumentNullException(nameof(getPropertyAttributeName));
        _mapToAttributes = mapToAttributes ?? throw new ArgumentNullException(nameof(mapToAttributes));
        _getVersion = getVersion ?? throw new ArgumentNullException(nameof(getVersion));
        RangeKeyAttributeName = rangeKeyAttributeName;
        _mapFromAttributes = mapFromAttributes;
    }

    /// <inheritdoc />
    public string TableName { get; }

    /// <inheritdoc />
    public string HashKeyAttributeName { get; }

    /// <inheritdoc />
    public string? RangeKeyAttributeName { get; }

    /// <inheritdoc />
    public string GetPropertyAttributeName(string propertyName)
    {
        return _getPropertyAttributeName(propertyName);
    }

    /// <inheritdoc />
    public Dictionary<string, AttributeValue> MapToAttributes(object obj, DynamoDBEntryConversion? conversion)
    {
        if (obj is T typed)
        {
            return _mapToAttributes(typed);
        }

        throw new ArgumentException(
            $"Expected instance of {typeof(T).FullName} but got {obj?.GetType().FullName ?? "null"}.",
            nameof(obj));
    }

    /// <inheritdoc />
    public (string? VersionProperty, object? Value) GetVersion(object item)
    {
        if (item is T typed)
        {
            return _getVersion(typed);
        }

        throw new ArgumentException(
            $"Expected instance of {typeof(T).FullName} but got {item?.GetType().FullName ?? "null"}.",
            nameof(item));
    }

    /// <inheritdoc />
    public object MapFromAttributes(Dictionary<string, AttributeValue> attributes)
    {
        if (_mapFromAttributes != null)
        {
            return _mapFromAttributes(attributes);
        }

        throw new NotSupportedException(
            $"Deserialization is not supported for type {typeof(T).FullName}. " +
            "Ensure the type is a partial class so the source generator can emit MapFromAttributes.");
    }
}
