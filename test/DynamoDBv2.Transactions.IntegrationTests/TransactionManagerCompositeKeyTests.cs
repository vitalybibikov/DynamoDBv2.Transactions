using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    [Collection("DynamoDb")]
    public class TransactionManagerCompositeKeyTests
    {
        private readonly DatabaseFixture _fixture;

        public TransactionManagerCompositeKeyTests(DatabaseFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task CreateOrUpdate_CompositeKey_WritesAndReadsBack()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "item-001";

            var item = new CompositeKeyTestTable
            {
                PartitionKey = pk,
                SortKey = sk,
                Status = "Active",
                Amount = 99.99m,
                IsActive = true
            };

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(item);
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(pk, sk);
            var result = await reader.ExecuteAsync();

            var data = result.GetItem<CompositeKeyTestTable>(0);
            Assert.NotNull(data);
            Assert.Equal(pk, data.PartitionKey);
            Assert.Equal(sk, data.SortKey);
            Assert.Equal("Active", data.Status);
            Assert.Equal(99.99m, data.Amount);
            Assert.True(data.IsActive);
            Assert.Equal(0, data.Version);
        }

        [Fact]
        public async Task CreateOrUpdate_MultipleItems_SameHashKey_DifferentSortKeys()
        {
            var pk = Guid.NewGuid().ToString();

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = "item-001",
                    Status = "First",
                    Amount = 10m
                });
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = "item-002",
                    Status = "Second",
                    Amount = 20m
                });
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = "item-003",
                    Status = "Third",
                    Amount = 30m
                });
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(pk, "item-001");
            reader.Get<CompositeKeyTestTable>(pk, "item-002");
            reader.Get<CompositeKeyTestTable>(pk, "item-003");
            var result = await reader.ExecuteAsync();

            Assert.Equal(3, result.Count);
            Assert.Equal("First", result.GetItem<CompositeKeyTestTable>(0)!.Status);
            Assert.Equal("Second", result.GetItem<CompositeKeyTestTable>(1)!.Status);
            Assert.Equal("Third", result.GetItem<CompositeKeyTestTable>(2)!.Status);
        }

        [Fact]
        public async Task Delete_CompositeKey_RemovesItem()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "to-delete";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Status = "ToDelete"
                });
            }

            // Verify it exists
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(pk, sk);
            var result = await reader.ExecuteAsync();
            Assert.NotNull(result.GetItem<CompositeKeyTestTable>(0));

            // Delete via composite key
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                var deleteRequest = new DeleteTransactionRequest<CompositeKeyTestTable>(pk, sk);
                writer.AddRawRequest(deleteRequest);
            }

            // Verify deleted
            var reader2 = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader2.Get<CompositeKeyTestTable>(pk, sk);
            var result2 = await reader2.ExecuteAsync();
            Assert.Null(result2.GetItem<CompositeKeyTestTable>(0));
        }

        [Fact]
        public async Task Patch_CompositeKey_UpdatesProperty()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "to-patch";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Status = "Original",
                    Amount = 50m
                });
            }

            // Patch via composite key
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                var patchRequest = new PatchTransactionRequest<CompositeKeyTestTable>(
                    pk, sk,
                    new Requests.Properties.Property { Name = "Status", Value = "Patched" });
                writer.AddRawRequest(patchRequest);
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(pk, sk);
            var result = await reader.ExecuteAsync();

            var data = result.GetItem<CompositeKeyTestTable>(0);
            Assert.NotNull(data);
            Assert.Equal("Patched", data.Status);
            Assert.Equal(50m, data.Amount); // unchanged
        }

        [Fact]
        public async Task ConditionEquals_CompositeKey_Passes()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "cond-check";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Amount = 100m
                });
            }

            // Should not throw — condition is met
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<CompositeKeyTestTable, decimal>(
                    pk, sk, x => x.Amount, 100m);
            }
        }

        [Fact]
        public async Task ConditionEquals_CompositeKey_Fails_Throws()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "cond-fail";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Amount = 100m
                });
            }

            await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
            {
                await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
                {
                    writer.ConditionEquals<CompositeKeyTestTable, decimal>(
                        pk, sk, x => x.Amount, 999m);
                }
            });
        }

        [Fact]
        public async Task ConditionGreaterThan_CompositeKey_Passes()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "gt-check";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Amount = 200m
                });
            }

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionGreaterThan<CompositeKeyTestTable, decimal>(
                    pk, sk, x => x.Amount, 100m);
            }
        }

        [Fact]
        public async Task ConditionLessThan_CompositeKey_Passes()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "lt-check";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Amount = 50m
                });
            }

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionLessThan<CompositeKeyTestTable, decimal>(
                    pk, sk, x => x.Amount, 100m);
            }
        }

        [Fact]
        public async Task ConditionNotEquals_CompositeKey_Passes()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "ne-check";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Amount = 75m
                });
            }

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionNotEquals<CompositeKeyTestTable, decimal>(
                    pk, sk, x => x.Amount, 100m);
            }
        }

        [Fact]
        public async Task ConditionVersionEquals_CompositeKey_Passes()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "ver-check";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Status = "Versioned"
                });
            }

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionVersionEquals<CompositeKeyTestTable>(
                    pk, sk, x => x.Version, 0);
            }
        }

        [Fact]
        public async Task ConditionVersionEquals_CompositeKey_Fails_Throws()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "ver-fail";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Status = "Versioned"
                });
            }

            await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
            {
                await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
                {
                    writer.ConditionVersionEquals<CompositeKeyTestTable>(
                        pk, sk, x => x.Version, 999);
                }
            });
        }

        [Fact]
        public async Task ReadWithProjection_CompositeKey_ReturnsOnlyProjectedFields()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "proj-item";

            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk,
                    Status = "Active",
                    Amount = 150m,
                    IsActive = true
                });
            }

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(pk, sk, x => new { x.Status });
            var result = await reader.ExecuteAsync();

            var data = result.GetItem<CompositeKeyTestTable>(0);
            Assert.NotNull(data);
            Assert.Equal("Active", data.Status);
            Assert.Equal(default(decimal), data.Amount); // not projected
        }

        [Fact]
        public async Task ReadNonExistentCompositeKey_ReturnsNull()
        {
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(Guid.NewGuid().ToString(), "nonexistent");
            var result = await reader.ExecuteAsync();

            Assert.Equal(1, result.Count);
            Assert.Null(result.GetItem<CompositeKeyTestTable>(0));
        }

        [Fact]
        public async Task VersionIncrement_CompositeKey_WorksCorrectly()
        {
            var pk = Guid.NewGuid().ToString();
            var sk = "ver-inc";

            var item = new CompositeKeyTestTable
            {
                PartitionKey = pk,
                SortKey = sk,
                Status = "v0"
            };

            // Write v0
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(item);
            }

            // Load, update, write v1
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(pk, sk);
            var result = await reader.ExecuteAsync();
            var loaded = result.GetItem<CompositeKeyTestTable>(0)!;
            Assert.Equal(0, loaded.Version);

            loaded.Status = "v1";
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(loaded);
            }

            // Read back
            var reader2 = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader2.Get<CompositeKeyTestTable>(pk, sk);
            var result2 = await reader2.ExecuteAsync();
            var reloaded = result2.GetItem<CompositeKeyTestTable>(0)!;

            Assert.Equal(1, reloaded.Version);
            Assert.Equal("v1", reloaded.Status);
        }

        [Fact]
        public async Task MultipleOps_CompositeKey_AtomicTransaction()
        {
            var pk = Guid.NewGuid().ToString();
            var sk1 = "atomic-1";
            var sk2 = "atomic-2";

            // Create two items
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk1,
                    Status = "Initial",
                    Amount = 100m
                });
                writer.CreateOrUpdate(new CompositeKeyTestTable
                {
                    PartitionKey = pk,
                    SortKey = sk2,
                    Status = "Initial",
                    Amount = 200m
                });
            }

            // Condition check on first + patch second atomically
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<CompositeKeyTestTable, string>(
                    pk, sk1, x => x.Status, "Initial");

                var patchRequest = new PatchTransactionRequest<CompositeKeyTestTable>(
                    pk, sk2,
                    new Requests.Properties.Property { Name = "Status", Value = "Updated" });
                writer.AddRawRequest(patchRequest);
            }

            // Verify
            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<CompositeKeyTestTable>(pk, sk2);
            var result = await reader.ExecuteAsync();
            Assert.Equal("Updated", result.GetItem<CompositeKeyTestTable>(0)!.Status);
        }
    }
}
