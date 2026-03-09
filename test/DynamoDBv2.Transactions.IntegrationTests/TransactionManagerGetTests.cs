using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.IntegrationTests.Helpers;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    [Collection("DynamoDb")]
    public class TransactionManagerGetTests
    {
        private readonly DatabaseFixture _fixture;

        public TransactionManagerGetTests(DatabaseFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task WriteAndReadBack_AllProperties()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            byte[] bytes = [1, 2, 3, 4, 5, 6, 7, 8];
            var memoryStream = new MemoryStream(bytes);

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeInt = 123456789,
                SomeNullableInt32 = 42,
                SomeLong = int.MaxValue,
                SomeNullableLong = 999,
                SomeFloat = 123.456f,
                SomeNullableFloat = 78.9f,
                SomeDecimal = 123456789.123m,
                SomeNullableDecimal = 456.789m,
                SomeDate = DateTime.UtcNow,
                SomeNullableDate1 = DateTime.UtcNow.AddDays(-1),
                SomeBool = true,
                SomeNullableBool = false,
                SomeClass = new SomeClass { X = "TestX", Y = "TestY" },
                SomeRecord = new SomeRecord { X = "RecordX", Y = "RecordY" },
                SomeClassList = new List<SomeClass> { new SomeClass { X = "ListX", Y = "ListY" } },
                SomeBytes = bytes,
                SomeMemoryStream = memoryStream,
                SomeClassDictionary = new Dictionary<string, SomeClass> { { "Key1", new SomeClass { X = "DictX", Y = "DictY" } } }
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
            }

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(userId1);
            var result = await reader.ExecuteAsync();

            // Assert
            Assert.Equal(1, result.Count);
            var data1 = result.GetItem<TestTable>(0);
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
            Assert.Equal(t1.SomeNullableDate1!.Value, data1.SomeNullableDate1!.Value, new DateTimeComparer(TimeSpan.FromMicroseconds(999)));
            Assert.Equal(t1.SomeBool, data1.SomeBool);
            Assert.Equal(t1.SomeNullableBool, data1.SomeNullableBool);
            Assert.Equal(t1.SomeClass.X, data1.SomeClass!.X);
            Assert.Equal(t1.SomeClass.Y, data1.SomeClass.Y);
            Assert.Equal(t1.SomeRecord.X, data1.SomeRecord!.X);
            Assert.Equal(t1.SomeRecord.Y, data1.SomeRecord.Y);
            Assert.Equal(t1.SomeClassList[0].X, data1.SomeClassList![0].X);
            Assert.Equal(t1.SomeClassList[0].Y, data1.SomeClassList[0].Y);
            Assert.Equal(t1.SomeClassDictionary["Key1"].X, data1.SomeClassDictionary!["Key1"].X);
            Assert.Equal(t1.SomeClassDictionary["Key1"].Y, data1.SomeClassDictionary["Key1"].Y);
            MemoryStreamsEquality.StreamEqual(t1.SomeMemoryStream, data1.SomeMemoryStream);
            Assert.Equal(t1.SomeBytes, data1.SomeBytes);
        }

        [Fact]
        public async Task WriteAndReadBack_NullablePropertiesAreNull()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeInt = 100,
                SomeNullableInt32 = null,
                SomeLong = 200,
                SomeNullableLong = null,
                SomeFloat = 1.5f,
                SomeNullableFloat = null,
                SomeDecimal = 2.5m,
                SomeNullableDecimal = null,
                SomeDate = DateTime.UtcNow,
                SomeNullableDate1 = null,
                SomeBool = false,
                SomeNullableBool = null,
                SomeClass = null,
                SomeRecord = null,
                SomeClassList = null,
                SomeClassDictionary = null,
                SomeMemoryStream = null,
                SomeBytes = null
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
            }

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(userId1);
            var result = await reader.ExecuteAsync();

            // Assert
            var data1 = result.GetItem<TestTable>(0);
            Assert.NotNull(data1);
            Assert.Equal(t1.UserId, data1.UserId);
            Assert.Equal(t1.SomeInt, data1.SomeInt);
            Assert.Null(data1.SomeNullableInt32);
            Assert.Null(data1.SomeNullableLong);
            Assert.Null(data1.SomeNullableFloat);
            Assert.Null(data1.SomeNullableDecimal);
            Assert.Null(data1.SomeNullableDate1);
            Assert.Null(data1.SomeNullableBool);
            Assert.Null(data1.SomeClass);
            Assert.Null(data1.SomeRecord);
            Assert.Null(data1.SomeClassList);
            Assert.Null(data1.SomeClassDictionary);
            Assert.Null(data1.SomeMemoryStream);
            Assert.Null(data1.SomeBytes);
        }

        [Fact]
        public async Task ReadNonExistentItem_ReturnsNull()
        {
            // Arrange
            var nonExistentUserId = Guid.NewGuid().ToString();

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(nonExistentUserId);
            var result = await reader.ExecuteAsync();

            // Assert
            Assert.Equal(1, result.Count);
            var data = result.GetItem<TestTable>(0);
            Assert.Null(data);
        }

        [Fact]
        public async Task ReadMultipleItems_ReturnsAll()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();
            var userId2 = Guid.NewGuid().ToString();
            var userId3 = Guid.NewGuid().ToString();

            var t1 = new TestTable { UserId = userId1, SomeInt = 111 };
            var t2 = new TestTable { UserId = userId2, SomeInt = 222 };
            var t3 = new TestTable { UserId = userId3, SomeInt = 333 };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
                writer.CreateOrUpdate(t2);
                writer.CreateOrUpdate(t3);
            }

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(userId1);
            reader.Get<TestTable>(userId2);
            reader.Get<TestTable>(userId3);
            var result = await reader.ExecuteAsync();

            // Assert
            Assert.Equal(3, result.Count);

            var data1 = result.GetItem<TestTable>(0);
            var data2 = result.GetItem<TestTable>(1);
            var data3 = result.GetItem<TestTable>(2);

            Assert.NotNull(data1);
            Assert.NotNull(data2);
            Assert.NotNull(data3);

            Assert.Equal(111, data1.SomeInt);
            Assert.Equal(222, data2.SomeInt);
            Assert.Equal(333, data3.SomeInt);

            Assert.Equal(userId1, data1.UserId);
            Assert.Equal(userId2, data2.UserId);
            Assert.Equal(userId3, data3.UserId);
        }

        [Fact]
        public async Task ReadWithProjection_ReturnsOnlyProjectedFields()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeInt = 42,
                SomeBool = true,
                SomeFloat = 3.14f,
                SomeDecimal = 99.99m,
                SomeDate = DateTime.UtcNow
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
            }

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(userId1, x => new { x.SomeInt, x.SomeBool });
            var result = await reader.ExecuteAsync();

            // Assert
            var data1 = result.GetItem<TestTable>(0);
            Assert.NotNull(data1);
            Assert.Equal(42, data1.SomeInt);
            Assert.True(data1.SomeBool);

            // Non-projected fields should be at their default values
            Assert.Equal(default(float), data1.SomeFloat);
            Assert.Equal(default(decimal), data1.SomeDecimal);
        }

        [Fact]
        public async Task ReadAfterUpdate_ReturnsUpdatedVersion()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeInt = 100
            };

            // Initial write
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
            }

            // Load to get version=0, then update
            var loaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId1);
            Assert.NotNull(loaded);
            Assert.Equal(0, loaded.Version);

            loaded.SomeInt = 200;

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(loaded);
            }

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(userId1);
            var result = await reader.ExecuteAsync();

            // Assert
            var data1 = result.GetItem<TestTable>(0);
            Assert.NotNull(data1);
            Assert.Equal(1, data1.Version);
            Assert.Equal(200, data1.SomeInt);
        }

        [Fact]
        public async Task WriteAndReadBack_VersionIsZeroForNewItem()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeInt = 555
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
            }

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<TestTable>(userId1);
            var result = await reader.ExecuteAsync();

            // Assert
            var data1 = result.GetItem<TestTable>(0);
            Assert.NotNull(data1);
            Assert.Equal(0, data1.Version);
            Assert.Equal(555, data1.SomeInt);
        }

        [Fact]
        public async Task ReadWithConsumedCapacity_ReturnsCapacityInfo()
        {
            // Arrange
            var userId1 = Guid.NewGuid().ToString();

            var t1 = new TestTable
            {
                UserId = userId1,
                SomeInt = 777
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(t1);
            }

            // Act
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Options = new ReadTransactionOptions
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
            };
            reader.Get<TestTable>(userId1);
            var result = await reader.ExecuteAsync();

            // Assert
            var data1 = result.GetItem<TestTable>(0);
            Assert.NotNull(data1);
            Assert.Equal(777, data1.SomeInt);

            Assert.NotNull(result.ConsumedCapacity);
            Assert.NotEmpty(result.ConsumedCapacity);
        }

        [Fact]
        public async Task Read100Items_Succeeds()
        {
            // Arrange - write 100 items in batches of 25 (DynamoDB transaction limit)
            const int totalItems = 100;
            var userIds = new string[totalItems];

            for (int i = 0; i < totalItems; i++)
            {
                userIds[i] = Guid.NewGuid().ToString();
            }

            // Write in batches of 25 (TransactWriteItems limit)
            const int writeBatchSize = 25;
            for (int batch = 0; batch < totalItems; batch += writeBatchSize)
            {
                await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
                {
                    for (int i = batch; i < batch + writeBatchSize && i < totalItems; i++)
                    {
                        writer.CreateOrUpdate(new TestTable
                        {
                            UserId = userIds[i],
                            SomeInt = i
                        });
                    }
                }
            }

            // Act - read all 100 in a single TransactGetItems call
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            for (int i = 0; i < totalItems; i++)
            {
                reader.Get<TestTable>(userIds[i]);
            }

            var result = await reader.ExecuteAsync();

            // Assert
            Assert.Equal(totalItems, result.Count);

            for (int i = 0; i < totalItems; i++)
            {
                var item = result.GetItem<TestTable>(i);
                Assert.NotNull(item);
                Assert.Equal(userIds[i], item.UserId);
                Assert.Equal(i, item.SomeInt);
            }
        }

        [Fact]
        public async Task Read101Items_ThrowsArgumentException()
        {
            // Arrange
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            for (int i = 0; i < 101; i++)
            {
                reader.Get<TestTable>(Guid.NewGuid().ToString());
            }

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await reader.ExecuteAsync();
            });
        }
    }
}
