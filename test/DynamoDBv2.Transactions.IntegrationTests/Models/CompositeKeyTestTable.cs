using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.IntegrationTests.Models
{
    [DynamoDBTable("CompositeKeyTestTable")]
    public class CompositeKeyTestTable : ITransactional
    {
        [DynamoDBHashKey("PK")]
        public string PartitionKey { get; set; } = "";

        [DynamoDBRangeKey("SK")]
        public string SortKey { get; set; } = "";

        [DynamoDBProperty("Status")]
        public string Status { get; set; } = "";

        [DynamoDBProperty("Amount")]
        public decimal Amount { get; set; }

        [DynamoDBProperty("IsActive")]
        public bool IsActive { get; set; }

        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
