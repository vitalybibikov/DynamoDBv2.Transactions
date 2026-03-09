using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    [Collection("DynamoDb")]
    public class TransactionManagerEnumTests
    {
        private readonly DatabaseFixture _fixture;

        public TransactionManagerEnumTests(DatabaseFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task WriteAndReadBack_EnumProperty_RoundTrips()
        {
            var entityId = Guid.NewGuid().ToString();

            var item = new EnumTestTable
            {
                EntityId = entityId,
                Status = IntegrationOrderStatus.Shipped,
                CreatedAt = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero),
                Description = "Enum test"
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(item);
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<EnumTestTable>(entityId);
            var result = await reader.ExecuteAsync();

            var data = result.GetItem<EnumTestTable>(0);
            Assert.NotNull(data);
            Assert.Equal(IntegrationOrderStatus.Shipped, data.Status);
            Assert.Equal("Enum test", data.Description);
        }

        [Fact]
        public async Task WriteAndReadBack_DateTimeOffsetProperty_RoundTrips()
        {
            var entityId = Guid.NewGuid().ToString();
            var createdAt = new DateTimeOffset(2025, 12, 25, 14, 30, 45, 123, TimeSpan.Zero);

            var item = new EnumTestTable
            {
                EntityId = entityId,
                Status = IntegrationOrderStatus.Pending,
                CreatedAt = createdAt,
                Description = "DateTimeOffset test"
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(item);
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<EnumTestTable>(entityId);
            var result = await reader.ExecuteAsync();

            var data = result.GetItem<EnumTestTable>(0);
            Assert.NotNull(data);
            // DateTimeOffset round-trips via ISO string — compare with ms precision
            Assert.Equal(createdAt.Year, data.CreatedAt.Year);
            Assert.Equal(createdAt.Month, data.CreatedAt.Month);
            Assert.Equal(createdAt.Day, data.CreatedAt.Day);
            Assert.Equal(createdAt.Hour, data.CreatedAt.Hour);
            Assert.Equal(createdAt.Minute, data.CreatedAt.Minute);
            Assert.Equal(createdAt.Second, data.CreatedAt.Second);
        }

        [Fact]
        public async Task WriteAndReadBack_AllEnumValues()
        {
            var statuses = Enum.GetValues<IntegrationOrderStatus>();

            foreach (var status in statuses)
            {
                var entityId = Guid.NewGuid().ToString();

                await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
                {
                    writer.CreateOrUpdate(new EnumTestTable
                    {
                        EntityId = entityId,
                        Status = status,
                        CreatedAt = DateTimeOffset.UtcNow
                    });
                }

                var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
                reader.Get<EnumTestTable>(entityId);
                var result = await reader.ExecuteAsync();

                var data = result.GetItem<EnumTestTable>(0);
                Assert.NotNull(data);
                Assert.Equal(status, data.Status);
            }
        }

        [Fact]
        public async Task Patch_EnumProperty_UpdatesCorrectly()
        {
            var entityId = Guid.NewGuid().ToString();

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new EnumTestTable
                {
                    EntityId = entityId,
                    Status = IntegrationOrderStatus.Pending,
                    CreatedAt = DateTimeOffset.UtcNow,
                    Description = "Will be patched"
                });
            }

            // Patch the status
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.PatchAsync<EnumTestTable, IntegrationOrderStatus>(
                    entityId, x => x.Status, IntegrationOrderStatus.Delivered);
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<EnumTestTable>(entityId);
            var result = await reader.ExecuteAsync();

            var data = result.GetItem<EnumTestTable>(0);
            Assert.NotNull(data);
            Assert.Equal(IntegrationOrderStatus.Delivered, data.Status);
            Assert.Equal("Will be patched", data.Description); // unchanged
        }

        [Fact]
        public async Task ConditionCheck_EnumProperty_Passes()
        {
            var entityId = Guid.NewGuid().ToString();

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new EnumTestTable
                {
                    EntityId = entityId,
                    Status = IntegrationOrderStatus.Confirmed,
                    CreatedAt = DateTimeOffset.UtcNow
                });
            }

            // Enum is stored as N (numeric). ConditionEquals should work.
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<EnumTestTable, IntegrationOrderStatus>(
                    entityId, x => x.Status, IntegrationOrderStatus.Confirmed);
            }
        }

        [Fact]
        public async Task ConditionCheck_EnumProperty_Fails_Throws()
        {
            var entityId = Guid.NewGuid().ToString();

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new EnumTestTable
                {
                    EntityId = entityId,
                    Status = IntegrationOrderStatus.Confirmed,
                    CreatedAt = DateTimeOffset.UtcNow
                });
            }

            await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
            {
                await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
                {
                    writer.ConditionEquals<EnumTestTable, IntegrationOrderStatus>(
                        entityId, x => x.Status, IntegrationOrderStatus.Cancelled);
                }
            });
        }

        [Fact]
        public async Task UpdateAndReadBack_EnumProperty_VersionIncrements()
        {
            var entityId = Guid.NewGuid().ToString();

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new EnumTestTable
                {
                    EntityId = entityId,
                    Status = IntegrationOrderStatus.Pending,
                    CreatedAt = DateTimeOffset.UtcNow
                });
            }

            // Read and update
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<EnumTestTable>(entityId);
            var result = await reader.ExecuteAsync();
            var loaded = result.GetItem<EnumTestTable>(0)!;
            Assert.Equal(0, loaded.Version);

            loaded.Status = IntegrationOrderStatus.Shipped;
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(loaded);
            }

            var reader2 = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader2.Get<EnumTestTable>(entityId);
            var result2 = await reader2.ExecuteAsync();
            var reloaded = result2.GetItem<EnumTestTable>(0)!;

            Assert.Equal(1, reloaded.Version);
            Assert.Equal(IntegrationOrderStatus.Shipped, reloaded.Status);
        }

        [Fact]
        public async Task ReadMultipleEnumItems_ReturnsAll()
        {
            var id1 = Guid.NewGuid().ToString();
            var id2 = Guid.NewGuid().ToString();
            var id3 = Guid.NewGuid().ToString();

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new EnumTestTable
                {
                    EntityId = id1,
                    Status = IntegrationOrderStatus.Pending,
                    CreatedAt = DateTimeOffset.UtcNow
                });
                writer.CreateOrUpdate(new EnumTestTable
                {
                    EntityId = id2,
                    Status = IntegrationOrderStatus.Shipped,
                    CreatedAt = DateTimeOffset.UtcNow
                });
                writer.CreateOrUpdate(new EnumTestTable
                {
                    EntityId = id3,
                    Status = IntegrationOrderStatus.Delivered,
                    CreatedAt = DateTimeOffset.UtcNow
                });
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<EnumTestTable>(id1);
            reader.Get<EnumTestTable>(id2);
            reader.Get<EnumTestTable>(id3);
            var result = await reader.ExecuteAsync();

            Assert.Equal(3, result.Count);
            Assert.Equal(IntegrationOrderStatus.Pending, result.GetItem<EnumTestTable>(0)!.Status);
            Assert.Equal(IntegrationOrderStatus.Shipped, result.GetItem<EnumTestTable>(1)!.Status);
            Assert.Equal(IntegrationOrderStatus.Delivered, result.GetItem<EnumTestTable>(2)!.Status);
        }
    }
}
