using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup;

/// <summary>
/// Entity with a version property using a custom attribute name.
/// </summary>
[DynamoDBTable("VersionRenameTable")]
public partial class VersionRenameTestEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";

    [DynamoDBVersion]
    [DynamoDBProperty(AttributeName = "ver_num")]
    public long? Version { get; set; }
}

/// <summary>
/// Entity with [DynamoDBIgnore] on some properties.
/// </summary>
[DynamoDBTable("IgnoreTable")]
public partial class IgnoreTestEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";

    [DynamoDBIgnore]
    public string SecretField { get; set; } = "should-not-serialize";

    [DynamoDBIgnore]
    public int ComputedValue { get; set; } = 42;

    public decimal Price { get; set; }
}

/// <summary>
/// Non-partial entity with DynamoDBIgnore for reflection fallback tests.
/// </summary>
[DynamoDBTable("IgnoreReflTable")]
public class IgnoreReflectionTestEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";

    [DynamoDBIgnore]
    public string InternalState { get; set; } = "internal";

    public int Amount { get; set; }
}

/// <summary>
/// Non-partial entity with custom version attribute name for reflection tests.
/// </summary>
[DynamoDBTable("VersionRenameReflTable")]
public class VersionRenameReflectionEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";

    [DynamoDBVersion]
    [DynamoDBProperty(AttributeName = "custom_version")]
    public long? Version { get; set; }
}
