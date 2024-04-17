using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests.Properties;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    [Collection("DynamoDb")]
    public class TransactionManagerDeleteTests
    {
        private readonly DatabaseFixture _fixture;

        public TransactionManagerDeleteTests(DatabaseFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task SaveDataToTableAndDelete_ctor1()
        {
            //Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeDate = DateTime.UtcNow,
                SomeDecimal = (decimal)123.45,
                SomeFloat = (float)123.45,
                SomeInt = 123
            };

            // Act
            await _fixture.Db.Context.SaveAsync(t1);

            await using (var writer = new DynamoDbTransactor(new TransactionManager(_fixture.Db.Client)))
            {
                writer.DeleteAsync<TestTable>(nameof(t1.UserId), userId1);
            }

            // Assert
            var data1 = await _fixture.Db.Context.LoadAsync<TestTable>(userId1);
            
            Assert.Null(data1);
        }

        [Fact]
        public async Task SaveDataToTableAndDelete_ctor2()
        {
            //Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeDate = DateTime.UtcNow,
                SomeDecimal = (decimal)123.45,
                SomeFloat = (float)123.45,
                SomeInt = 123
            };

            // Act
            await _fixture.Db.Context.SaveAsync(t1);

            await using (var writer = new DynamoDbTransactor(new TransactionManager(_fixture.Db.Client)))
            {
                writer.DeleteAsync<TestTable, string>(table => t1.UserId, userId1);
            }

            // Assert
            var data1 = await _fixture.Db.Context.LoadAsync<TestTable>(userId1);

            Assert.Null(data1);
        }


        [Fact]
        public async Task DeleteNonExistingItem()
        {
            // Arrange
            var nonExistingKey = "NonExistingKey";

            // Act
            await using (var writer = new DynamoDbTransactor(new TransactionManager(_fixture.Db.Client)))
            {
                writer.DeleteAsync<TestTable>(nameof(TestTable.UserId), nonExistingKey);
            }

            // Assert
            // No assertion, ensure the operation doesn't throw an exception
        }

        [Fact]
        public async Task DeleteItemWithNullKey()
        {
            // Arrange
            string nullKey = null;

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                await using (var writer = new DynamoDbTransactor(new TransactionManager(_fixture.Db.Client)))
                {
                    writer.DeleteAsync<TestTable>(nullKey, "SomeValue");
                }
            });
        }
    }
}
