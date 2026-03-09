using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests for condition check operations: verifies that condition expressions
/// in our library correctly evaluate against data written by either system.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonConditionCheckTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonConditionCheckTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ConditionEquals_SdkWritten_LibCheckPasses()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 42, SomeLong = 1, SomeFloat = 123.456f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        // Should not throw — condition is met
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionEquals<TestTable, float>(userId, t => t.SomeFloat, 123.456f);
        }
    }

    [Fact]
    public async Task ConditionEquals_SdkWritten_LibCheckFails()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 42, SomeLong = 1, SomeFloat = 123.456f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<TestTable, float>(userId, t => t.SomeFloat, 999f);
            }
        });
    }

    [Fact]
    public async Task ConditionGreaterThan_SdkWritten_LibCheckPasses()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 100, SomeLong = int.MaxValue, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionGreaterThan<TestTable, long>(userId, t => t.SomeLong, (long)int.MaxValue - 1);
        }
    }

    [Fact]
    public async Task ConditionGreaterThan_SdkWritten_LibCheckFails()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 100, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionGreaterThan<TestTable, int>(userId, t => t.SomeInt, 100);
            }
        });
    }

    [Fact]
    public async Task ConditionLessThan_SdkWritten_LibCheckPasses()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 50, SomeLong = int.MaxValue, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionLessThan<TestTable, long>(userId, t => t.SomeLong, (long)int.MaxValue + 1);
        }
    }

    [Fact]
    public async Task ConditionLessThan_SdkWritten_LibCheckFails()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 100, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionLessThan<TestTable, int>(userId, t => t.SomeInt, 100);
            }
        });
    }

    [Fact]
    public async Task ConditionNotEquals_SdkWritten_LibCheckPasses()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 42, SomeLong = int.MaxValue, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionNotEquals<TestTable, long>(userId, t => t.SomeLong, 0);
        }
    }

    [Fact]
    public async Task ConditionNotEquals_SdkWritten_LibCheckFails()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 42, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionNotEquals<TestTable, int>(userId, t => t.SomeInt, 42);
            }
        });
    }

    [Fact]
    public async Task ConditionVersionEquals_SdkWritten_LibCheckPasses()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        // SDK writes version=0 for new items
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionVersionEquals<TestTable>(userId, t => t.Version, 0);
        }
    }

    [Fact]
    public async Task ConditionVersionEquals_SdkWritten_LibCheckFails()
    {
        var userId = Guid.NewGuid().ToString();
        await _fixture.Db.Context.SaveAsync(new TestTable { UserId = userId, SomeInt = 1, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionVersionEquals<TestTable>(userId, t => t.Version, 999);
            }
        });
    }

    [Fact]
    public async Task ConditionEquals_LibWritten_LibCheckPasses()
    {
        var userId = Guid.NewGuid().ToString();
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 42, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 77.7m, SomeDate = DateTime.UtcNow });
        }

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionEquals<TestTable, decimal>(userId, t => t.SomeDecimal, 77.7m);
        }
    }

    [Fact]
    public async Task ConditionCheck_AfterCrossSystemUpdate_ReflectsNewValue()
    {
        var userId = Guid.NewGuid().ToString();

        // Lib writes
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 10, SomeLong = 1, SomeFloat = 1f, SomeDecimal = 1m, SomeDate = DateTime.UtcNow });
        }

        // SDK updates
        var loaded = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
        loaded.SomeInt = 99;
        await _fixture.Db.Context.SaveAsync(loaded);

        // Lib condition check should see updated value
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionEquals<TestTable, int>(userId, t => t.SomeInt, 99);
        }

        // Old value should fail
        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<TestTable, int>(userId, t => t.SomeInt, 10);
            }
        });
    }

    [Fact]
    public async Task ConditionCheck_CompositeKey_SdkWritten_LibCheckPasses()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "cond-sdk";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Amount = 500m });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.ConditionEquals<CompositeKeyTestTable, decimal>(pk, sk, x => x.Amount, 500m);
        }
    }

    [Fact]
    public async Task ConditionCheck_CompositeKey_SdkWritten_LibCheckFails()
    {
        var pk = Guid.NewGuid().ToString();
        var sk = "cond-fail-sdk";

        await _fixture.Db.Context.SaveAsync(new CompositeKeyTestTable { PartitionKey = pk, SortKey = sk, Amount = 500m });

        await Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
            {
                writer.ConditionEquals<CompositeKeyTestTable, decimal>(pk, sk, x => x.Amount, 999m);
            }
        });
    }
}
