using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests.Properties;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    [Collection("DynamoDb")]
    public class TransactionManagerConditionCheckTests
    {
        private readonly DatabaseFixture _fixture;

        public TransactionManagerConditionCheckTests(DatabaseFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task SaveDataAndCheckTheyAreTheSame_ctor1()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            byte[] bytes = [1, 2, 3, 4, 5, 6, 7, 8];
            var memoryStream = new MemoryStream(bytes);

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeInt = 123456789,
                SomeNullableInt32 = null,
                SomeLong = int.MaxValue,
                SomeNullableLong = null,
                SomeFloat = 123.456f,
                SomeNullableFloat = null,
                SomeDecimal = 123456789.123m,
                SomeNullableDecimal = null,
                SomeDate = DateTime.UtcNow,
                SomeNullableDate1 = null,
                SomeBool = true,
                SomeNullableBool = null,
                SomeClass = new SomeClass { X = "TestX", Y = "TestY" },
                SomeRecord = new SomeRecord { X = "RecordX", Y = "RecordY" },
                SomeClassList = new List<SomeClass> { new SomeClass { X = "ListX", Y = "ListY" } },
                SomeBytes = bytes,
                SomeMemoryStream = memoryStream,
                SomeClassDictionary = new Dictionary<string, SomeClass> { { "Key1", new SomeClass { X = "DictX", Y = "DictY" } } }
            };

            // Act
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
            };

            var keyValue = new KeyValue { Key = nameof(TestTable.UserId), Value = userId1 };
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<TestTable, float>(keyValue,table => t1.SomeFloat, 123.456f);
            };
        }
    }
}
