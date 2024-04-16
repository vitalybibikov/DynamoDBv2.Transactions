using System;
using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup
{
    public class SomeNotAttributedDynamoDbEntity : ITransactional
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Status { get; set; }

        public double Amount { get; set; }

        public long? Version { get; set; }
    }
}
