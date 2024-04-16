using System;

namespace DynamoDBv2.Transactions.IntegrationTests.Models
{
    public record SomeRecord
    {
        public string X { get; set; }

        public string Y { get; set; }
    }
}
