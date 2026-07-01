using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    /// <summary>
    /// End-to-end tests for <see cref="DynamoDbItemWriter"/> — single-item, non-transactional writes
    /// executed as plain <c>UpdateItem</c>/<c>PutItem</c>/<c>DeleteItem</c>. Verifies attribute-scoped
    /// patching, atomic version bump, the <c>attribute_exists</c> guard surfacing as
    /// <see cref="ConditionalCheckFailedException"/> (NOT <c>TransactionCanceledException</c>), and the
    /// motivating guarantee: a single-item write is never aborted by a concurrent non-transactional
    /// writer (no <c>TransactionConflict</c>).
    /// </summary>
    [Collection("DynamoDb")]
    public class DynamoDbItemWriterTests
    {
        private readonly DatabaseFixture _fixture;
        private readonly DynamoDbItemWriter _writer;

        public DynamoDbItemWriterTests(DatabaseFixture fixture)
        {
            _fixture = fixture;
            _writer = new DynamoDbItemWriter(_fixture.Db.Client);
        }

        private static TestTable NewItem(string userId) => new()
        {
            UserId = userId,
            SomeInt = 100,
            SomeNullableInt32 = 5,
            SomeDecimal = 10.5m,
            SomeFloat = 1.5f,
            SomeBool = true,
            SomeDate = DateTime.UtcNow
        };

        [Fact]
        public async Task CreateOrUpdateAsync_CreatesItem()
        {
            var userId = Guid.NewGuid().ToString();

            await _writer.CreateOrUpdateAsync(NewItem(userId));

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.NotNull(data);
            Assert.Equal(100, data.SomeInt);
        }

        [Fact]
        public async Task PatchAsync_SingleAttribute_UpdatesOnlyThatAttribute()
        {
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            item.SomeInt = 555;
            await _writer.PatchAsync(item, nameof(TestTable.SomeInt));

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(555, data.SomeInt);
            Assert.Equal(10.5m, data.SomeDecimal);
        }

        [Fact]
        public async Task PatchAsync_Many_WithIncrementVersion_BumpsVersion_LeavesOthersUntouched()
        {
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            item.SomeInt = 200;
            item.SomeDecimal = 99.9m;
            await _writer.PatchAsync(item, incrementVersion: true,
                nameof(TestTable.SomeInt), nameof(TestTable.SomeDecimal));

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(200, data.SomeInt);
            Assert.Equal(99.9m, data.SomeDecimal);
            Assert.True(data.SomeBool); // untouched
            Assert.Equal(1, data.Version);
        }

        [Fact]
        public async Task PatchAsync_Many_WithoutIncrement_LeavesVersionUnchanged()
        {
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            item.SomeInt = 200;
            await _writer.PatchAsync(item, incrementVersion: false, nameof(TestTable.SomeInt));

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(200, data.SomeInt);
            Assert.Equal(0, data.Version);
        }

        [Fact]
        public async Task PatchAsync_OnNonExistentItem_Throws_ConditionalCheckFailed_NotTransactionCanceled()
        {
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId); // never saved

            item.SomeInt = 1;
            // A plain UpdateItem surfaces the attribute_exists guard as ConditionalCheckFailedException,
            // never as a TransactionCanceledException — this is the exception-contract difference from
            // the transactional path.
            await Assert.ThrowsAsync<ConditionalCheckFailedException>(
                () => _writer.PatchAsync(item, incrementVersion: true, nameof(TestTable.SomeInt)));

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Null(data);
        }

        [Fact]
        public async Task DeleteAsync_RemovesItem()
        {
            var userId = Guid.NewGuid().ToString();
            await _fixture.Db.Context.SaveAsync(NewItem(userId));

            await _writer.DeleteAsync<TestTable>(userId);

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Null(data);
        }

        [Fact]
        public async Task PatchAsync_CompositeKey_PatchesCorrectItem()
        {
            var pk = Guid.NewGuid().ToString();
            var item = new CompositeKeyTestTable
            {
                PartitionKey = pk,
                SortKey = "sk-1",
                Status = "Open",
                Amount = 1m,
                IsActive = true
            };
            await _fixture.Db.Context.SaveAsync(item);

            item.Status = "Closed";
            item.Amount = 250m;
            await _writer.PatchAsync(item, incrementVersion: true,
                nameof(CompositeKeyTestTable.Status), nameof(CompositeKeyTestTable.Amount));

            var data = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "sk-1");
            Assert.Equal("Closed", data.Status);
            Assert.Equal(250m, data.Amount);
            Assert.True(data.IsActive);
            Assert.Equal(1, data.Version);
        }

        [Fact]
        public async Task DeleteAsync_CompositeKey_RemovesItem()
        {
            var pk = Guid.NewGuid().ToString();
            var item = new CompositeKeyTestTable { PartitionKey = pk, SortKey = "sk-1", Status = "Open" };
            await _fixture.Db.Context.SaveAsync(item);

            await _writer.DeleteAsync<CompositeKeyTestTable>(pk, "sk-1");

            var data = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "sk-1");
            Assert.Null(data);
        }

        [Fact]
        public async Task TwoDisjointAttributePatches_Concurrent_BothApply()
        {
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            var a = NewItem(userId);
            a.SomeInt = 777;
            var b = NewItem(userId);
            b.SomeDecimal = 55.5m;

            await Task.WhenAll(
                _writer.PatchAsync(a, incrementVersion: true, nameof(TestTable.SomeInt)),
                _writer.PatchAsync(b, incrementVersion: true, nameof(TestTable.SomeDecimal)));

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(777, data.SomeInt);
            Assert.Equal(55.5m, data.SomeDecimal);
            Assert.Equal(2, data.Version); // both bumps applied
        }

        [Fact]
        public async Task Patch_UnderConcurrentPlainPut_NeverRaisesTransactionConflict()
        {
            // The motivating guarantee: because the writer uses a plain UpdateItem (not TransactWriteItems),
            // a concurrent non-transactional write to the same item can NEVER abort it with a
            // TransactionConflict — it just serializes at the item level.
            var userId = Guid.NewGuid().ToString();
            await _fixture.Db.Context.SaveAsync(NewItem(userId));

            var tableName = DynamoDbMapper.GetTableName(typeof(TestTable));

            for (var i = 0; i < 25; i++)
            {
                var patch = NewItem(userId);
                patch.SomeInt = i;

                var rawPut = _fixture.Db.Client.PutItemAsync(new PutItemRequest
                {
                    TableName = tableName,
                    Item = new Dictionary<string, AttributeValue>
                    {
                        ["UserId"] = new AttributeValue { S = userId }
                    }
                });

                // Neither the patch nor the interleaved plain put must throw a transaction-conflict error.
                var ex = await Record.ExceptionAsync(() => Task.WhenAll(
                    _writer.PatchAsync(patch, incrementVersion: true, nameof(TestTable.SomeInt)),
                    rawPut));

                Assert.False(ex is TransactionCanceledException, $"unexpected transaction conflict: {ex}");
            }
        }
    }
}
