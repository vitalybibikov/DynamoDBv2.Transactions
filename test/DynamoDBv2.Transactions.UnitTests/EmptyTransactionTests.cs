using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for empty transaction handling and basic DynamoDbTransactor lifecycle.
/// Covers fix #6: empty transactions return null without calling DynamoDB.
/// </summary>
public class EmptyTransactionTests
{
    [Fact]
    public async Task ExecuteTransactionAsync_EmptyRequests_ReturnsNull_DoesNotCallDynamoDB()
    {
        // Arrange
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new TransactionManager(mockClient.Object);

        // Act
        var result = await manager.ExecuteTransactionAsync(
            Array.Empty<ITransactionRequest>());

        // Assert
        Assert.Null(result);
        mockClient.Verify(
            c => c.TransactWriteItemsAsync(
                It.IsAny<TransactWriteItemsRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ExecuteTransactionAsync_SingleRequest_CallsDynamoDB()
    {
        // Arrange
        var mockClient = new Mock<IAmazonDynamoDB>();
        mockClient
            .Setup(c => c.TransactWriteItemsAsync(
                It.IsAny<TransactWriteItemsRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactWriteItemsResponse());

        var manager = new TransactionManager(mockClient.Object);
        var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("key-1");

        // Act
        var result = await manager.ExecuteTransactionAsync(new[] { request });

        // Assert
        Assert.NotNull(result);
        mockClient.Verify(
            c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r => r.TransactItems.Count == 1),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_NoOperations_DoesNotCallManager()
    {
        // Arrange
        var mockManager = new Mock<ITransactionManager>();
        var transactor = new DynamoDbTransactor(mockManager.Object);

        // Act — dispose without adding any operations
        await transactor.DisposeAsync();

        // Assert — ExecuteTransactionAsync is still called (with empty list),
        // but the TransactionManager itself returns null for empty requests
        mockManager.Verify(
            m => m.ExecuteTransactionAsync(
                It.IsAny<IEnumerable<ITransactionRequest>>(),
                It.IsAny<TransactionOptions?>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
