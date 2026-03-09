using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.IntegrationTests.Models
{
    public enum IntegrationOrderStatus
    {
        Pending = 0,
        Confirmed = 1,
        Shipped = 2,
        Delivered = 3,
        Cancelled = 4
    }

    [DynamoDBTable("EnumTestTable")]
    public class EnumTestTable : ITransactional
    {
        [DynamoDBHashKey("EntityId")]
        public string EntityId { get; set; } = "";

        [DynamoDBProperty("Status")]
        public IntegrationOrderStatus Status { get; set; }

        [DynamoDBProperty("CreatedAt")]
        public DateTimeOffset CreatedAt { get; set; }

        [DynamoDBProperty("Description")]
        public string Description { get; set; } = "";

        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
