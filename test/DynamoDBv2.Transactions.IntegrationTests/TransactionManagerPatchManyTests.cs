using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests
{
    /// <summary>
    /// End-to-end tests for the multi-attribute patch (<c>PatchAsync(model, incrementVersion, params propertyNames)</c>).
    /// Verifies against a real DynamoDB endpoint that only the listed attributes are written, that the
    /// optional atomic version increment behaves as an <c>ADD</c> (never a version-equality condition), that a
    /// missing item is rejected by the <c>attribute_exists</c> guard, and that two writers touching disjoint
    /// attributes of the same item never conflict.
    /// </summary>
    [Collection("DynamoDb")]
    public class TransactionManagerPatchManyTests
    {
        private readonly DatabaseFixture _fixture;

        public TransactionManagerPatchManyTests(DatabaseFixture fixture)
        {
            _fixture = fixture;
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
        public async Task PatchMany_SetsOnlyListedAttributes_LeavesOthersUntouched()
        {
            // Arrange
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            // Act — patch two attributes, leave SomeInt/SomeBool untouched.
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                item.SomeDecimal = 99.9m;
                item.SomeFloat = 42.0f;
                writer.PatchAsync(item, incrementVersion: false, nameof(TestTable.SomeDecimal), nameof(TestTable.SomeFloat));
            }

            // Assert
            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.NotNull(data);
            Assert.Equal(99.9m, data.SomeDecimal);
            Assert.Equal(42.0f, data.SomeFloat);
            // untouched
            Assert.Equal(100, data.SomeInt);
            Assert.True(data.SomeBool);
            Assert.Equal(5, data.SomeNullableInt32);
        }

        [Fact]
        public async Task PatchMany_WithIncrementVersion_BumpsVersionByOne()
        {
            // Arrange
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            var before = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(0, before.Version);

            // Act
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                item.SomeInt = 200;
                writer.PatchAsync(item, incrementVersion: true, nameof(TestTable.SomeInt));
            }

            // Assert
            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(200, data.SomeInt);
            Assert.Equal(1, data.Version);
        }

        [Fact]
        public async Task PatchMany_WithoutIncrementVersion_LeavesVersionUnchanged()
        {
            // Arrange
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            // Act
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                item.SomeInt = 200;
                writer.PatchAsync(item, incrementVersion: false, nameof(TestTable.SomeInt));
            }

            // Assert
            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(200, data.SomeInt);
            Assert.Equal(0, data.Version);
        }

        [Fact]
        public async Task PatchMany_OnNonExistentItem_Throws_AndDoesNotCreateItem()
        {
            // Arrange — item is never saved; attribute_exists(hashKey) must reject the patch.
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);

            // Act & Assert
            await Assert.ThrowsAnyAsync<AmazonDynamoDBException>(async () =>
            {
                await using var writer = new DynamoDbTransactor(_fixture.Db.Client);
                item.SomeInt = 200;
                writer.PatchAsync(item, incrementVersion: true, nameof(TestTable.SomeInt));
            });

            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Null(data);
        }

        [Fact]
        public async Task PatchMany_TwoDisjointAttributePatches_BothPersist_NoConflict()
        {
            // Arrange
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            // Act — two independent writers each patch a different attribute with an atomic version bump.
            // Because neither uses a version-equality condition, both succeed regardless of ordering.
            await using (var writer1 = new DynamoDbTransactor(_fixture.Db.Client))
            {
                item.SomeInt = 777;
                writer1.PatchAsync(item, incrementVersion: true, nameof(TestTable.SomeInt));
            }

            await using (var writer2 = new DynamoDbTransactor(_fixture.Db.Client))
            {
                item.SomeDecimal = 55.5m;
                writer2.PatchAsync(item, incrementVersion: true, nameof(TestTable.SomeDecimal));
            }

            // Assert — both attribute writes landed and the version was bumped twice.
            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Equal(777, data.SomeInt);
            Assert.Equal(55.5m, data.SomeDecimal);
            Assert.Equal(2, data.Version);
        }

        [Fact]
        public async Task PatchMany_NullValuedAttribute_WritesNull()
        {
            // Arrange
            var userId = Guid.NewGuid().ToString();
            var item = NewItem(userId);
            await _fixture.Db.Context.SaveAsync(item);

            // Act — patch a nullable attribute to null.
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                item.SomeNullableInt32 = null;
                writer.PatchAsync(item, incrementVersion: false, nameof(TestTable.SomeNullableInt32));
            }

            // Assert
            var data = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
            Assert.Null(data.SomeNullableInt32);
        }

        [Fact]
        public async Task PatchMany_CompositeKey_PatchesCorrectItem()
        {
            // Arrange
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

            // Act
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                item.Status = "Closed";
                item.Amount = 250m;
                writer.PatchAsync(item, incrementVersion: true, nameof(CompositeKeyTestTable.Status), nameof(CompositeKeyTestTable.Amount));
            }

            // Assert
            var data = await _fixture.Db.Context.LoadAsync<CompositeKeyTestTable>(pk, "sk-1");
            Assert.NotNull(data);
            Assert.Equal("Closed", data.Status);
            Assert.Equal(250m, data.Amount);
            Assert.True(data.IsActive);
            Assert.Equal(1, data.Version);
        }
    }
}
