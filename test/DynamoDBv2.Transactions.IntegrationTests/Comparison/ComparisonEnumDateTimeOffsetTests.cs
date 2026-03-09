using DynamoDBv2.Transactions.IntegrationTests.Models;
using DynamoDBv2.Transactions.IntegrationTests.Setup;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using Xunit;

namespace DynamoDBv2.Transactions.IntegrationTests.Comparison;

/// <summary>
/// Comparison tests for enum and DateTimeOffset serialization:
/// verifies that our library stores enums and DateTimeOffset values identically to the AWS SDK.
/// </summary>
[Collection("DynamoDb")]
public class ComparisonEnumDateTimeOffsetTests
{
    private readonly DatabaseFixture _fixture;

    public ComparisonEnumDateTimeOffsetTests(DatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Enum_LibWrite_SdkRead_RoundTrips()
    {
        var entityId = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new EnumTestTable
            {
                EntityId = entityId,
                Status = IntegrationOrderStatus.Shipped,
                CreatedAt = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero),
                Description = "Lib enum write"
            });
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<EnumTestTable>(entityId);
        Assert.NotNull(sdkResult);
        Assert.Equal(IntegrationOrderStatus.Shipped, sdkResult.Status);
        Assert.Equal("Lib enum write", sdkResult.Description);
    }

    [Fact]
    public async Task Enum_SdkWrite_LibRead_RoundTrips()
    {
        var entityId = Guid.NewGuid().ToString();

        await _fixture.Db.Context.SaveAsync(new EnumTestTable
        {
            EntityId = entityId,
            Status = IntegrationOrderStatus.Confirmed,
            CreatedAt = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero),
            Description = "SDK enum write"
        });

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<EnumTestTable>(entityId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<EnumTestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(IntegrationOrderStatus.Confirmed, libResult.Status);
        Assert.Equal("SDK enum write", libResult.Description);
    }

    [Fact]
    public async Task Enum_AllValues_LibWrite_SdkRead_AllMatch()
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

            var sdkResult = await _fixture.Db.Context.LoadAsync<EnumTestTable>(entityId);
            Assert.NotNull(sdkResult);
            Assert.Equal(status, sdkResult.Status);
        }
    }

    [Fact]
    public async Task Enum_AllValues_SdkWrite_LibRead_AllMatch()
    {
        var statuses = Enum.GetValues<IntegrationOrderStatus>();

        foreach (var status in statuses)
        {
            var entityId = Guid.NewGuid().ToString();

            await _fixture.Db.Context.SaveAsync(new EnumTestTable
            {
                EntityId = entityId,
                Status = status,
                CreatedAt = DateTimeOffset.UtcNow
            });

            var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
            reader.Get<EnumTestTable>(entityId);
            var result = await reader.ExecuteAsync();
            var libResult = result.GetItem<EnumTestTable>(0);

            Assert.NotNull(libResult);
            Assert.Equal(status, libResult.Status);
        }
    }

    [Fact]
    public async Task Enum_BothReadPaths_ReturnSameValue()
    {
        var entityId = Guid.NewGuid().ToString();

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new EnumTestTable
            {
                EntityId = entityId,
                Status = IntegrationOrderStatus.Delivered,
                CreatedAt = DateTimeOffset.UtcNow
            });
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<EnumTestTable>(entityId);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<EnumTestTable>(entityId);
        var libResult = (await reader.ExecuteAsync()).GetItem<EnumTestTable>(0);

        Assert.Equal(sdkResult.Status, libResult!.Status);
    }

    [Fact]
    public async Task Enum_Patch_LibPatch_SdkVerify()
    {
        var entityId = Guid.NewGuid().ToString();

        await _fixture.Db.Context.SaveAsync(new EnumTestTable
        {
            EntityId = entityId,
            Status = IntegrationOrderStatus.Pending,
            CreatedAt = DateTimeOffset.UtcNow,
            Description = "Pre-patch"
        });

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.PatchAsync<EnumTestTable, IntegrationOrderStatus>(
                entityId, x => x.Status, IntegrationOrderStatus.Cancelled);
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<EnumTestTable>(entityId);
        Assert.Equal(IntegrationOrderStatus.Cancelled, sdkResult.Status);
        Assert.Equal("Pre-patch", sdkResult.Description); // unchanged
    }

    [Fact]
    public async Task DateTimeOffset_LibWrite_SdkRead_RoundTrips()
    {
        var entityId = Guid.NewGuid().ToString();
        var createdAt = new DateTimeOffset(2025, 12, 25, 14, 30, 45, 123, TimeSpan.Zero);

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new EnumTestTable
            {
                EntityId = entityId,
                Status = IntegrationOrderStatus.Pending,
                CreatedAt = createdAt,
                Description = "DTO test"
            });
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<EnumTestTable>(entityId);
        Assert.NotNull(sdkResult);
        // Compare with second precision (ISO string may lose sub-ms)
        Assert.Equal(createdAt.Year, sdkResult.CreatedAt.Year);
        Assert.Equal(createdAt.Month, sdkResult.CreatedAt.Month);
        Assert.Equal(createdAt.Day, sdkResult.CreatedAt.Day);
        Assert.Equal(createdAt.Hour, sdkResult.CreatedAt.Hour);
        Assert.Equal(createdAt.Minute, sdkResult.CreatedAt.Minute);
        Assert.Equal(createdAt.Second, sdkResult.CreatedAt.Second);
    }

    [Fact]
    public async Task DateTimeOffset_SdkWrite_LibRead_RoundTrips()
    {
        var entityId = Guid.NewGuid().ToString();
        var createdAt = new DateTimeOffset(2025, 7, 4, 12, 0, 0, TimeSpan.Zero);

        await _fixture.Db.Context.SaveAsync(new EnumTestTable
        {
            EntityId = entityId,
            Status = IntegrationOrderStatus.Confirmed,
            CreatedAt = createdAt,
            Description = "SDK DTO test"
        });

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<EnumTestTable>(entityId);
        var result = await reader.ExecuteAsync();
        var libResult = result.GetItem<EnumTestTable>(0);

        Assert.NotNull(libResult);
        Assert.Equal(createdAt.Year, libResult.CreatedAt.Year);
        Assert.Equal(createdAt.Month, libResult.CreatedAt.Month);
        Assert.Equal(createdAt.Day, libResult.CreatedAt.Day);
        Assert.Equal(createdAt.Hour, libResult.CreatedAt.Hour);
        Assert.Equal(createdAt.Minute, libResult.CreatedAt.Minute);
        Assert.Equal(createdAt.Second, libResult.CreatedAt.Second);
    }

    [Fact]
    public async Task DateTimeOffset_BothReadPaths_ReturnSameValue()
    {
        var entityId = Guid.NewGuid().ToString();
        var createdAt = new DateTimeOffset(2025, 3, 15, 8, 45, 30, TimeSpan.Zero);

        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new EnumTestTable
            {
                EntityId = entityId,
                Status = IntegrationOrderStatus.Shipped,
                CreatedAt = createdAt
            });
        }

        var sdkResult = await _fixture.Db.Context.LoadAsync<EnumTestTable>(entityId);

        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<EnumTestTable>(entityId);
        var libResult = (await reader.ExecuteAsync()).GetItem<EnumTestTable>(0);

        Assert.NotNull(sdkResult);
        Assert.NotNull(libResult);
        // Both should agree on the stored value
        Assert.Equal(sdkResult.CreatedAt.Year, libResult.CreatedAt.Year);
        Assert.Equal(sdkResult.CreatedAt.Month, libResult.CreatedAt.Month);
        Assert.Equal(sdkResult.CreatedAt.Day, libResult.CreatedAt.Day);
        Assert.Equal(sdkResult.CreatedAt.Hour, libResult.CreatedAt.Hour);
        Assert.Equal(sdkResult.CreatedAt.Minute, libResult.CreatedAt.Minute);
        Assert.Equal(sdkResult.CreatedAt.Second, libResult.CreatedAt.Second);
    }

    [Fact]
    public async Task Enum_VersionIncrement_CrossSystem()
    {
        var entityId = Guid.NewGuid().ToString();

        // Lib writes v0
        await using (var writer = new DynamoDbTransactor(_fixture.Db.Client))
        {
            writer.CreateOrUpdate(new EnumTestTable
            {
                EntityId = entityId,
                Status = IntegrationOrderStatus.Pending,
                CreatedAt = DateTimeOffset.UtcNow
            });
        }

        // SDK reads v0
        var sdkLoaded = await _fixture.Db.Context.LoadAsync<EnumTestTable>(entityId);
        Assert.Equal(0, sdkLoaded.Version);
        Assert.Equal(IntegrationOrderStatus.Pending, sdkLoaded.Status);

        // SDK updates to v1
        sdkLoaded.Status = IntegrationOrderStatus.Shipped;
        await _fixture.Db.Context.SaveAsync(sdkLoaded);

        // Lib reads v1
        var reader = new DynamoDbReadTransactor(_fixture.Db.Client);
        reader.Get<EnumTestTable>(entityId);
        var libResult = (await reader.ExecuteAsync()).GetItem<EnumTestTable>(0);

        Assert.Equal(1, libResult!.Version);
        Assert.Equal(IntegrationOrderStatus.Shipped, libResult.Status);
    }
}
