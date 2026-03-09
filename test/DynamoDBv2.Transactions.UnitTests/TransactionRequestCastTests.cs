using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests;

/// <summary>
/// Tests for fix #7: TransactionManager validates that all requests inherit
/// from TransactionRequest (the abstract base) rather than merely implementing
/// ITransactionRequest.
/// </summary>
public class TransactionRequestCastTests
{
    [Fact]
    public async Task ExecuteTransactionAsync_NonTransactionRequest_ThrowsArgumentException()
    {
        // Arrange
        var mockClient = new Mock<IAmazonDynamoDB>();
        var manager = new TransactionManager(mockClient.Object);

        // Create a mock that only implements ITransactionRequest but does NOT
        // inherit from TransactionRequest (the abstract base class).
        var fakeRequest = new Mock<ITransactionRequest>();
        fakeRequest.Setup(r => r.TableName).Returns("FakeTable");
        fakeRequest.Setup(r => r.Key).Returns(new Dictionary<string, AttributeValue>());
        fakeRequest.Setup(r => r.Type).Returns(TransactOperationType.Delete);

        // Act & Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => manager.ExecuteTransactionAsync(new[] { fakeRequest.Object }));

        Assert.Contains("TransactionRequest", ex.Message);
    }

    [Fact]
    public async Task ExecuteTransactionAsync_ValidTransactionRequest_Works()
    {
        // Arrange
        var mockClient = new Mock<IAmazonDynamoDB>();
        mockClient
            .Setup(c => c.TransactWriteItemsAsync(
                It.IsAny<TransactWriteItemsRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TransactWriteItemsResponse());

        var manager = new TransactionManager(mockClient.Object);

        // A concrete TransactionRequest subclass
        var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("key-1");

        // Act
        var result = await manager.ExecuteTransactionAsync(new[] { request });

        // Assert
        Assert.NotNull(result);
        mockClient.Verify(
            c => c.TransactWriteItemsAsync(
                It.IsAny<TransactWriteItemsRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
