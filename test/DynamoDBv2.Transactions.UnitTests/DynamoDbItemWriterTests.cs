using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for <see cref="DynamoDbItemWriter"/> — verifies each public method builds the correct request
/// type (reusing the same request classes as the transactor) and dispatches it once through
/// <see cref="ISingleWriteManager"/>, plus argument guards and construction.
/// </summary>
public class DynamoDbItemWriterTests
{
    private readonly Mock<ISingleWriteManager> _manager = new();
    private ITransactionRequest? _captured;

    private DynamoDbItemWriter CreateWriter()
    {
        _manager
            .Setup(m => m.ExecuteAsync(It.IsAny<ITransactionRequest>(), It.IsAny<CancellationToken>()))
            .Callback<ITransactionRequest, CancellationToken>((r, _) => _captured = r)
            .Returns(Task.CompletedTask);

        return new DynamoDbItemWriter(_manager.Object);
    }

    private static OrderTestEntity CreateOrder() => new()
    {
        OrderId = "order-1",
        SortKey = "sort-1",
        Status = "Closed",
        Total = 42.5,
        CustomerName = "Alice",
        Version = 7
    };

    [Fact]
    public async Task CreateOrUpdateAsync_BuildsPutRequest()
    {
        var writer = CreateWriter();
        await writer.CreateOrUpdateAsync(CreateOrder());

        var request = Assert.IsType<PutTransactionRequest<OrderTestEntity>>(_captured);
        Assert.Equal(TransactOperationType.Put, request.Type);
        _manager.Verify(m => m.ExecuteAsync(It.IsAny<ITransactionRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PatchAsync_SingleProperty_BuildsPatchRequest()
    {
        var writer = CreateWriter();
        await writer.PatchAsync(CreateOrder(), "Status");

        Assert.IsType<PatchTransactionRequest<OrderTestEntity>>(_captured);
    }

    [Fact]
    public async Task PatchAsync_ParamsOverload_BuildsPatchManyRequest_WithAddVersion()
    {
        var writer = CreateWriter();
        await writer.PatchAsync(CreateOrder(), incrementVersion: true, "Status", "Total");

        var request = Assert.IsType<PatchManyTransactionRequest<OrderTestEntity>>(_captured);
        Assert.Contains("ADD #version :increment", request.UpdateExpression!);
    }

    [Fact]
    public async Task PatchAsync_CollectionOverload_BuildsPatchManyRequest()
    {
        var writer = CreateWriter();
        await writer.PatchAsync(CreateOrder(), incrementVersion: false, new[] { "Status" }, CancellationToken.None);

        var request = Assert.IsType<PatchManyTransactionRequest<OrderTestEntity>>(_captured);
        Assert.DoesNotContain("ADD", request.UpdateExpression!);
    }

    [Fact]
    public async Task PatchAsync_Expression_BuildsPatchRequest()
    {
        var writer = CreateWriter();
        await writer.PatchAsync<OrderTestEntity, string>("order-1", o => o.Status, "New");

        Assert.IsType<PatchTransactionRequest<OrderTestEntity>>(_captured);
    }

    [Fact]
    public async Task PatchAsync_Expression_NullKey_Throws()
    {
        var writer = CreateWriter();
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => writer.PatchAsync<OrderTestEntity, string>(null!, o => o.Status, "New"));
    }

    [Fact]
    public async Task PatchAsync_Expression_NonMember_Throws()
    {
        var writer = CreateWriter();
        await Assert.ThrowsAsync<ArgumentException>(
            () => writer.PatchAsync<OrderTestEntity, string>("order-1", o => "constant", "New"));
    }

    [Fact]
    public async Task DeleteAsync_HashKey_BuildsDeleteRequest()
    {
        var writer = CreateWriter();
        await writer.DeleteAsync<ProductTestEntity>("prod-1");

        Assert.IsType<DeleteTransactionRequest<ProductTestEntity>>(_captured);
    }

    [Fact]
    public async Task DeleteAsync_CompositeKey_BuildsDeleteRequest()
    {
        var writer = CreateWriter();
        await writer.DeleteAsync<OrderTestEntity>("order-1", "sort-1");

        Assert.IsType<DeleteTransactionRequest<OrderTestEntity>>(_captured);
    }

    [Fact]
    public async Task DeleteAsync_NullHashKey_Throws()
    {
        var writer = CreateWriter();
        await Assert.ThrowsAsync<ArgumentNullException>(() => writer.DeleteAsync<ProductTestEntity>(null!));
    }

    [Fact]
    public async Task DeleteAsync_Composite_NullRangeKey_Throws()
    {
        var writer = CreateWriter();
        await Assert.ThrowsAsync<ArgumentNullException>(() => writer.DeleteAsync<OrderTestEntity>("order-1", null!));
    }

    [Fact]
    public async Task WriteRawAsync_PassesRequestThrough()
    {
        var writer = CreateWriter();
        var raw = new PatchTransactionRequest<OrderTestEntity>(CreateOrder(), "Status");

        await writer.WriteRawAsync(raw);

        Assert.Same(raw, _captured);
    }

    [Fact]
    public async Task WriteRawAsync_Null_Throws()
    {
        var writer = CreateWriter();
        await Assert.ThrowsAsync<ArgumentNullException>(() => writer.WriteRawAsync(null!));
    }

    [Fact]
    public void Constructor_NullManager_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new DynamoDbItemWriter((ISingleWriteManager)null!));
    }

    [Fact]
    public void Constructor_WithClient_Constructs()
    {
        var client = new Mock<IAmazonDynamoDB>().Object;
        var writer = new DynamoDbItemWriter(client);
        Assert.NotNull(writer);
    }
}
