using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Setup
{
    public class DatabaseFixture : IAsyncLifetime
    {
        public AwsDBContextProvider Db { get; private set; }

        public DatabaseFixture()
        {
            Db = new AwsDBContextProvider();
        }

        public async Task InitializeAsync()
        {
            await CreateTable();
        }

        private async Task CreateTable()
        {
            try
            {
                await Db.Client.CreateTableAsync(
                    new CreateTableRequest(nameof(TestTable), 
                        [new(nameof(TestTable.UserId), KeyType.HASH)], 
                        [new (nameof(TestTable.UserId), ScalarAttributeType.S)],
                        new ProvisionedThroughput { ReadCapacityUnits = 1, WriteCapacityUnits = 1})
                    );
            }
            catch (Exception e)
            {
            }
        }

        public async Task DisposeAsync()
        {
           //await Db.Client.DeleteTableAsync(nameof(TestTable));
        }
    }
}
