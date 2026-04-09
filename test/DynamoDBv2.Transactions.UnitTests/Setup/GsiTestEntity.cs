using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.UnitTests.Setup
{
    /// <summary>
    /// Entity with Global Secondary Index attributes that previously caused
    /// AmbiguousMatchException in reflection-based property name resolution.
    /// </summary>
    [DynamoDBTable("GsiTestEntity")]
    public class GsiTestEntity : ITransactional
    {
        [DynamoDBHashKey("BucketId")]
        public string BucketId { get; set; }

        [DynamoDBRangeKey("PlayerId")]
        [DynamoDBGlobalSecondaryIndexHashKey("PlayerId-CreatedTimeUtcString-index")]
        public string PlayerId { get; set; }

        [DynamoDBProperty("Position")]
        public int Position { get; set; }

        [DynamoDBGlobalSecondaryIndexRangeKey("PlayerId-CreatedTimeUtcString-index")]
        [DynamoDBProperty("CreatedTimeUtcString")]
        public string CreatedTimeUtcString { get; set; }

        [DynamoDBProperty("WasClaimed")]
        public bool WasClaimed { get; set; }

        [DynamoDBProperty("TTL")]
        public double TTL { get; set; }

        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
