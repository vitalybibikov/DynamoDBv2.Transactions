using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    public class TestTransactions
    {
        public async Task Add()
        {
            var client = new AmazonDynamoDBClient();

            var t1 = new TestTable()
            {
                UserId = "1",
                SomeDate = DateTime.UtcNow,
                SomeDecimal = (decimal)123.45,
                SomeFloat = (float)123.45,
                SomeInt = 123,
                Version = 1
            };
            var t2 = new TestTable()
            {
                UserId = "2",
                SomeDate = DateTime.UtcNow,
                SomeDecimal = (decimal)123.45,
                SomeFloat = (float)123.45,
                SomeInt = 123,
                Version = 1
            };

            await using (var writer = new TransactionalWriter(new TransactionManager(client)))
            {
                writer.CreateOrUpdate(t1);
                writer.CreateOrUpdate(t2);
            }

            await using (var writer2 = new TransactionalWriter(new TransactionManager(client)))
            {
                t1.SomeDate = DateTime.UtcNow;

                writer2.PatchAsync<TestTable, int>(new KeyValue
                {
                    Key = nameof(TestTable.UserId),
                    Value = 2.ToString()
                }, new Property
                {
                    Name = nameof(TestTable.SomeInt),
                    Value = 256
                });

                writer2.CreateOrUpdate(t1);
            };
        }
    }
}
