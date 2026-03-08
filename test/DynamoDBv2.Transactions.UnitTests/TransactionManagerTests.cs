using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests for TransactionManager covering all operation type mappings,
    /// cancellation token propagation, and edge cases.
    /// </summary>
    public class TransactionManagerTests
    {
        private readonly Mock<IAmazonDynamoDB> _mockClient;
        private readonly TransactionManager _manager;

        public TransactionManagerTests()
        {
            _mockClient = new Mock<IAmazonDynamoDB>();
            _manager = new TransactionManager(_mockClient.Object);
        }

        [Fact]
        public async Task ExecuteTransaction_PutOperation_MappedCorrectly()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test" };
            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            await _manager.ExecuteTransactionAsync(new[] { request });

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.TransactItems.Count == 1 &&
                    r.TransactItems[0].Put != null &&
                    r.TransactItems[0].Put.TableName == "SomeDynamoDbEntity"),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_DeleteOperation_MappedCorrectly()
        {
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("val1");

            await _manager.ExecuteTransactionAsync(new[] { request });

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.TransactItems.Count == 1 &&
                    r.TransactItems[0].Delete != null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_UpdateOperation_MappedCorrectly()
        {
            var request = new UpdateTransactionRequest<SomeDynamoDbEntity>("SET #a = :v");

            await _manager.ExecuteTransactionAsync(new[] { request });

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.TransactItems.Count == 1 &&
                    r.TransactItems[0].Update != null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_ConditionCheckOperation_MappedCorrectly()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key1");
            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");

            await _manager.ExecuteTransactionAsync(new[] { request });

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.TransactItems.Count == 1 &&
                    r.TransactItems[0].ConditionCheck != null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_PatchOperation_MappedToUpdate()
        {
            var request = new PatchTransactionRequest<SomeDynamoDbEntity>(
                new KeyValue { Key = "Id", Value = "1" },
                new Property { Name = "Status", Value = "Updated" });

            await _manager.ExecuteTransactionAsync(new[] { request });

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.TransactItems.Count == 1 &&
                    r.TransactItems[0].Update != null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_CancellationToken_Propagated()
        {
            var cts = new CancellationTokenSource();
            var token = cts.Token;
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("val");

            await _manager.ExecuteTransactionAsync(new[] { request }, token);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.IsAny<TransactWriteItemsRequest>(),
                token), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_EmptyRequests_StillCallsDynamoDB()
        {
            await _manager.ExecuteTransactionAsync(Array.Empty<ITransactionRequest>());

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r => r.TransactItems.Count == 0),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_AllFiveTypes_InCorrectOrder()
        {
            var requests = new ITransactionRequest[]
            {
                new PutTransactionRequest<SomeDynamoDbEntity>(new SomeDynamoDbEntity { Id = "1" }),
                new DeleteTransactionRequest<SomeDynamoDbEntity>("2"),
                new UpdateTransactionRequest<SomeDynamoDbEntity>("SET #a = :v"),
                new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("3"),
                new PatchTransactionRequest<SomeDynamoDbEntity>(
                    new KeyValue { Key = "Id", Value = "4" },
                    new Property { Name = "Status", Value = "x" }),
            };

            await _manager.ExecuteTransactionAsync(requests);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.TransactItems.Count == 5 &&
                    r.TransactItems[0].Put != null &&
                    r.TransactItems[1].Delete != null &&
                    r.TransactItems[2].Update != null &&
                    r.TransactItems[3].ConditionCheck != null &&
                    r.TransactItems[4].Update != null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_ReturnsResponse()
        {
            var expectedResponse = new TransactWriteItemsResponse();
            _mockClient.Setup(c => c.TransactWriteItemsAsync(
                It.IsAny<TransactWriteItemsRequest>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(expectedResponse);

            var result = await _manager.ExecuteTransactionAsync(
                new[] { new DeleteTransactionRequest<SomeDynamoDbEntity>("1") });

            Assert.Same(expectedResponse, result);
        }

        [Fact]
        public async Task ExecuteTransaction_Over100Items_ThrowsArgumentException()
        {
            var requests = Enumerable.Range(0, 101)
                .Select(i => new DeleteTransactionRequest<SomeDynamoDbEntity>(i.ToString()))
                .Cast<ITransactionRequest>()
                .ToList();

            var ex = await Assert.ThrowsAsync<ArgumentException>(
                () => _manager.ExecuteTransactionAsync(requests));

            Assert.Contains("100", ex.Message);
            Assert.Contains("101", ex.Message);
        }

        [Fact]
        public async Task ExecuteTransaction_Exactly100Items_Succeeds()
        {
            var requests = Enumerable.Range(0, 100)
                .Select(i => new DeleteTransactionRequest<SomeDynamoDbEntity>(i.ToString()))
                .Cast<ITransactionRequest>()
                .ToList();

            await _manager.ExecuteTransactionAsync(requests);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r => r.TransactItems.Count == 100),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_WithClientRequestToken_SetsToken()
        {
            var options = new TransactionOptions { ClientRequestToken = "idempotency-token-123" };

            await _manager.ExecuteTransactionAsync(
                new[] { new DeleteTransactionRequest<SomeDynamoDbEntity>("1") },
                options);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.ClientRequestToken == "idempotency-token-123"),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_WithReturnConsumedCapacity_SetsCapacity()
        {
            var options = new TransactionOptions
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
            };

            await _manager.ExecuteTransactionAsync(
                new[] { new DeleteTransactionRequest<SomeDynamoDbEntity>("1") },
                options);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.ReturnConsumedCapacity == ReturnConsumedCapacity.TOTAL),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_WithReturnItemCollectionMetrics_SetsMetrics()
        {
            var options = new TransactionOptions
            {
                ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.SIZE
            };

            await _manager.ExecuteTransactionAsync(
                new[] { new DeleteTransactionRequest<SomeDynamoDbEntity>("1") },
                options);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.ReturnItemCollectionMetrics == ReturnItemCollectionMetrics.SIZE),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_WithAllOptions_SetsAll()
        {
            var options = new TransactionOptions
            {
                ClientRequestToken = "token-abc",
                ReturnConsumedCapacity = ReturnConsumedCapacity.INDEXES,
                ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.SIZE
            };

            await _manager.ExecuteTransactionAsync(
                new[] { new DeleteTransactionRequest<SomeDynamoDbEntity>("1") },
                options);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.ClientRequestToken == "token-abc" &&
                    r.ReturnConsumedCapacity == ReturnConsumedCapacity.INDEXES &&
                    r.ReturnItemCollectionMetrics == ReturnItemCollectionMetrics.SIZE),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_NullOptions_DoesNotSetAnything()
        {
            await _manager.ExecuteTransactionAsync(
                new[] { new DeleteTransactionRequest<SomeDynamoDbEntity>("1") },
                options: null);

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.ClientRequestToken == null),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteTransaction_SimpleOverload_DelegatesToOptionsOverload()
        {
            await _manager.ExecuteTransactionAsync(
                new[] { new DeleteTransactionRequest<SomeDynamoDbEntity>("1") });

            _mockClient.Verify(c => c.TransactWriteItemsAsync(
                It.Is<TransactWriteItemsRequest>(r =>
                    r.ClientRequestToken == null),
                It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}
