using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup;

public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2,
    Delivered = 3,
    Cancelled = 4
}

public enum Priority
{
    Low = 0,
    Normal = 1,
    High = 2,
    Urgent = 3
}

/// <summary>
/// Partial entity with enum and DateTimeOffset for source-gen tests.
/// </summary>
[DynamoDBTable("EnumTable")]
public partial class EnumTestEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public OrderStatus Status { get; set; }
    public Priority Priority { get; set; }
    public OrderStatus? NullableStatus { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? UpdatedAt { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
/// Non-partial for reflection fallback tests.
/// </summary>
[DynamoDBTable("EnumReflTable")]
public class EnumReflectionTestEntity
{
    [DynamoDBHashKey(AttributeName = "pk")]
    public string Id { get; set; } = "";

    public OrderStatus Status { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string Name { get; set; } = "";
}
