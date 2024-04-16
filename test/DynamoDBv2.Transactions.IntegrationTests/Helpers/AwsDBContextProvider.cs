using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.IntegrationTests.Helpers
{
    public class AwsDBContextProvider
    {
        public AmazonDynamoDBClient Client { get; init; }
        public IDynamoDBContext Context { get; init; }

        public AwsDBContextProvider()
        {
            Client = GetDbClient();
            Context = new DynamoDBContext(Client);
        }

        private AmazonDynamoDBClient GetDbClient()
        {
            var config = new AmazonDynamoDBConfig
            {
                ServiceURL = "http://127.0.0.1:4566"
            };
            var client = new AmazonDynamoDBClient("dummy", "dummy", config);
            
            return client;
        }
    }
}
