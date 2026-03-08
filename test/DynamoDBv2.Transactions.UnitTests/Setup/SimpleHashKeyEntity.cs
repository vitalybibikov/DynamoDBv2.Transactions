using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup
{
    /// <summary>
    /// Entity with [DynamoDBHashKey] but without custom AttributeName.
    /// Used to test that property name is used when AttributeName is not specified.
    /// </summary>
    public class SimpleHashKeyEntity
    {
        [DynamoDBHashKey]
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";
    }
}
