using System;
using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup
{
    [DynamoDBTable("SomeDynamoDbEntity")]
    public class SomeDynamoDbEntity : ITransactional
    {
        [DynamoDBHashKey(AttributeName = "MyId")]
        public string Id { get; set; }

        [DynamoDBProperty(AttributeName = "Name")]
        public string Name { get; set; }

        public string Status { get; set; }

        public double Amount { get; set; }

        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
