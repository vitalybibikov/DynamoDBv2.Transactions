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
            await CreateCompositeKeyTable();
            await CreateEnumTestTable();
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

        private async Task CreateCompositeKeyTable()
        {
            try
            {
                await Db.Client.CreateTableAsync(
                    new CreateTableRequest(nameof(CompositeKeyTestTable),
                        [
                            new KeySchemaElement("PK", KeyType.HASH),
                            new KeySchemaElement("SK", KeyType.RANGE)
                        ],
                        [
                            new AttributeDefinition("PK", ScalarAttributeType.S),
                            new AttributeDefinition("SK", ScalarAttributeType.S)
                        ],
                        new ProvisionedThroughput { ReadCapacityUnits = 1, WriteCapacityUnits = 1 })
                    );
            }
            catch (Exception)
            {
            }
        }

        private async Task CreateEnumTestTable()
        {
            try
            {
                await Db.Client.CreateTableAsync(
                    new CreateTableRequest(nameof(EnumTestTable),
                        [new KeySchemaElement("EntityId", KeyType.HASH)],
                        [new AttributeDefinition("EntityId", ScalarAttributeType.S)],
                        new ProvisionedThroughput { ReadCapacityUnits = 1, WriteCapacityUnits = 1 })
                    );
            }
            catch (Exception)
            {
            }
        }

        public async Task DisposeAsync()
        {
           //await Db.Client.DeleteTableAsync(nameof(TestTable));
        }
    }
}
