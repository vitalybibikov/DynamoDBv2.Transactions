using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{

    [Collection("DynamoDb")]
    public class DynamoDbTransactorMultiOperations
    {
        private readonly DatabaseFixture _fixture;

        public DynamoDbTransactorMultiOperations(DatabaseFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task SaveDataAndCheckTheyAreTheSame()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();
            var userId2 = Guid.NewGuid().ToString();


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

            var t2 = new TestTable
            {
                UserId = userId2,
                SomeDate = DateTime.UtcNow,
                SomeDecimal = 500.5m,
                SomeFloat = (float)123.45,
                SomeInt = 123
            };

            // Act
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
                writer.PatchAsync<TestTable, decimal>(userId2, table => table.SomeDecimal, 500.5m);
            }

            // Assert
            var data1 = await _fixture.Db.Context.LoadAsync<TestTable>(userId1);
            var data2 = await _fixture.Db.Context.LoadAsync<TestTable>(userId2);

            Assert.NotNull(data1);


            Assert.Equal(t1.UserId, data1.UserId);
            Assert.Equal(t1.SomeInt, data1.SomeInt);
            Assert.Equal(t1.SomeNullableInt32, data1.SomeNullableInt32);
            Assert.Equal(t1.SomeLong, data1.SomeLong);
            Assert.Equal(t1.SomeNullableLong, data1.SomeNullableLong);
            Assert.Equal(t1.SomeFloat, data1.SomeFloat);
            Assert.Equal(t1.SomeNullableFloat, data1.SomeNullableFloat);
            Assert.Equal(t1.SomeDecimal, data1.SomeDecimal);
            Assert.Equal(t1.SomeNullableDecimal, data1.SomeNullableDecimal);
            Assert.Equal(t1.SomeDate, data1.SomeDate, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
            Assert.Equal(t1.SomeNullableDate1, data1.SomeNullableDate1);
            Assert.Equal(t1.SomeBool, data1.SomeBool);
            Assert.Equal(t1.SomeNullableBool, data1.SomeNullableBool);
            Assert.Equal(t1.SomeClass.X, data1.SomeClass.X);
            Assert.Equal(t1.SomeClass.Y, data1.SomeClass.Y);
            Assert.Equal(t1.SomeRecord.X, data1.SomeRecord.X);
            Assert.Equal(t1.SomeRecord.Y, data1.SomeRecord.Y);
            Assert.Equal(t1.SomeClassList[0].X, data1.SomeClassList[0].X);
            Assert.Equal(t1.SomeClassList[0].Y, data1.SomeClassList[0].Y);
            Assert.Equal(t1.SomeClassDictionary["Key1"].X, data1.SomeClassDictionary["Key1"].X);
            Assert.Equal(t1.SomeClassDictionary["Key1"].Y, data1.SomeClassDictionary["Key1"].Y);
            MemoryStreamsEquality.StreamEqual(t1.SomeMemoryStream, data1.SomeMemoryStream);
            Assert.Equal(t1.SomeBytes, data1.SomeBytes);
            Assert.Equal(t1.SomeClassDictionary["Key1"].Y, data1.SomeClassDictionary["Key1"].Y);
            Assert.Equal(0, data1.Version);

            Assert.NotNull(data2);
            Assert.Equal(t2.SomeDecimal, data2.SomeDecimal);
        }

        [Fact]
        public async Task SaveDataAndFailAmazonDynamoDbException()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeDate = DateTime.UtcNow,
                SomeDecimal = 500.5m,
                SomeFloat = (float)123.45,
                SomeInt = 123
            };

            // Assert and Act
            await Assert.ThrowsAsync<AmazonDynamoDBException>(async () =>
            {
                await using var dbTransactor = new DynamoDbTransactor(_fixture.Db.Client);
                dbTransactor.CreateOrUpdate(t1);
                dbTransactor.PatchAsync<TestTable, decimal>(userId1, table => table.SomeDecimal, 500.5m);
            });
        }
    }
}
