using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions
{
    public interface ITransactional
    {
        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
