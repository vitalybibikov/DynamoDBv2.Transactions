using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup;

/// <summary>
/// Simple nested class for complex type deserialization tests.
/// </summary>
public class InnerTestClass
{
    public string X { get; set; } = "";
    public string Y { get; set; } = "";
}

/// <summary>
/// Non-partial entity with complex types (nested class, list, dictionary, byte[], MemoryStream).
/// Uses reflection fallback path for deserialization.
/// </summary>
[DynamoDBTable("ComplexTypes")]
public class ComplexTypesTestEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";

    public InnerTestClass? Nested { get; set; }

    public List<InnerTestClass>? Items { get; set; }

    public Dictionary<string, InnerTestClass>? ItemMap { get; set; }

    public List<string>? Tags { get; set; }

    public List<int>? Numbers { get; set; }

    public Dictionary<string, string>? Metadata { get; set; }

    public byte[]? BinaryData { get; set; }

    public MemoryStream? StreamData { get; set; }
}
