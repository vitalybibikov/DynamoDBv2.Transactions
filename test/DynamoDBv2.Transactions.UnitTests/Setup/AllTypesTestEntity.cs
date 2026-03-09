using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup
{
    /// <summary>
    /// Entity with every primitive type supported by DynamoDbMapper, used for comprehensive
    /// deserialization and round-trip tests. Partial to trigger source generation.
    /// </summary>
    [DynamoDBTable("AllTypes")]
    public partial class AllTypesTestEntity
    {
        [DynamoDBHashKey(AttributeName = "pk")]
        public string Id { get; set; } = "";

        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public decimal DecimalValue { get; set; }
        public float FloatValue { get; set; }
        public double DoubleValue { get; set; }
        public bool BoolValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
        public char CharValue { get; set; }
        public string StringValue { get; set; } = "";

        public int? NullableInt { get; set; }
        public long? NullableLong { get; set; }
        public decimal? NullableDecimal { get; set; }
        public double? NullableDouble { get; set; }
        public bool? NullableBool { get; set; }
        public DateTime? NullableDateTime { get; set; }

        [DynamoDBVersion]
        public long? Version { get; set; }
    }

    /// <summary>
    /// Non-partial entity with all types, for reflection fallback deserialization tests.
    /// </summary>
    [DynamoDBTable("AllTypesReflection")]
    public class AllTypesReflectionEntity
    {
        [DynamoDBHashKey(AttributeName = "pk")]
        public string Id { get; set; } = "";

        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public decimal DecimalValue { get; set; }
        public float FloatValue { get; set; }
        public double DoubleValue { get; set; }
        public bool BoolValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
        public char CharValue { get; set; }
        public string StringValue { get; set; } = "";

        public int? NullableInt { get; set; }
        public long? NullableLong { get; set; }
        public decimal? NullableDecimal { get; set; }
        public double? NullableDouble { get; set; }
        public bool? NullableBool { get; set; }
        public DateTime? NullableDateTime { get; set; }

        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
