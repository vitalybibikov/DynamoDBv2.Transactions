using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests that DynamoDbTransactor.DisposeAsync is idempotent — calling it
/// multiple times only executes the transaction once (fix #13: dispose guard).
/// </summary>
public class DisposeIdempotencyTests
{
    private readonly Mock<ITransactionManager> _mockManager;

    public DisposeIdempotencyTests()
    {
        _mockManager = new Mock<ITransactionManager>();
        _mockManager.Setup(m => m.ExecuteTransactionAsync(
                It.IsAny<IEnumerable<ITransactionRequest>>(),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((TransactWriteItemsResponse?)null);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_ExecutesOnlyOnce()
    {
        var transactor = new DynamoDbTransactor(_mockManager.Object);

        await transactor.DisposeAsync();
        await transactor.DisposeAsync();

        _mockManager.Verify(
            m => m.ExecuteTransactionAsync(
                It.IsAny<IEnumerable<ITransactionRequest>>(),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_CalledThreeTimes_ExecutesOnlyOnce()
    {
        var transactor = new DynamoDbTransactor(_mockManager.Object);

        await transactor.DisposeAsync();
        await transactor.DisposeAsync();
        await transactor.DisposeAsync();

        _mockManager.Verify(
            m => m.ExecuteTransactionAsync(
                It.IsAny<IEnumerable<ITransactionRequest>>(),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_AfterError_NeverExecutes()
    {
        var transactor = new DynamoDbTransactor(_mockManager.Object);

        // Force an error by passing null model to PatchAsync
        try
        {
            transactor.PatchAsync<SomeDynamoDbEntity>(null!, "Status");
        }
        catch (ArgumentNullException)
        {
            // Expected
        }

        Assert.True(transactor.ErrorDuringExecution);

        // Dispose twice — neither should trigger execution
        await transactor.DisposeAsync();
        await transactor.DisposeAsync();

        _mockManager.Verify(
            m => m.ExecuteTransactionAsync(
                It.IsAny<IEnumerable<ITransactionRequest>>(),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task DisposeAsync_WithOperations_CalledTwice_ExecutesOnce()
    {
        var transactor = new DynamoDbTransactor(_mockManager.Object);
        transactor.CreateOrUpdate(new SomeDynamoDbEntity { Id = "1", Name = "Test" });

        await transactor.DisposeAsync();
        await transactor.DisposeAsync();

        _mockManager.Verify(
            m => m.ExecuteTransactionAsync(
                It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 1),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
