using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests for DynamoDbTransactor lifecycle: happy paths, error recovery,
    /// multi-operation transactions, and argument validation.
    /// </summary>
    public class TransactorLifecycleTests
    {
        private readonly Mock<ITransactionManager> _mockManager;

        public TransactorLifecycleTests()
        {
            _mockManager = new Mock<ITransactionManager>();
        }

        [Fact]
        public async Task FullLifecycle_CreateOrUpdate_ExecutesOnDispose()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Status = "Active", Amount = 99.9 };

            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                transactor.CreateOrUpdate(entity);
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 1),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task FullLifecycle_MultipleOperations_AllPassedToManager()
        {
            var entity1 = new SomeDynamoDbEntity { Id = "1", Name = "Test1" };
            var entity2 = new SomeDynamoDbEntity { Id = "2", Name = "Test2" };

            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                transactor.CreateOrUpdate(entity1);
                transactor.CreateOrUpdate(entity2);
                transactor.DeleteAsync<SomeDynamoDbEntity>("key3");
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 3),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task FullLifecycle_MixedOperationTypes_AllExecuted()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Status = "Active", Amount = 100 };

            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                transactor.CreateOrUpdate(entity);
                transactor.PatchAsync(entity, "Status");
                transactor.DeleteAsync<SomeDynamoDbEntity>("deleteKey");
                transactor.ConditionEquals<SomeDynamoDbEntity, string>("condKey", x => x.Status, "Active");
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 4),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ErrorDuringExecution_PreventsTransactionExecution()
        {
            var transactor = new Mock<DynamoDbTransactor>(_mockManager.Object) { CallBase = true };
            transactor.Setup(t => t.AddRawRequest(It.IsAny<ITransactionRequest>())).Throws(new InvalidOperationException());

            try { transactor.Object.CreateOrUpdate(new SomeDynamoDbEntity { Id = "1" }); }
            catch { }

            Assert.True(transactor.Object.ErrorDuringExecution);

            await transactor.Object.DisposeAsync();

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(It.IsAny<IEnumerable<ITransactionRequest>>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public void CreateOrUpdate_NullItem_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() => transactor.CreateOrUpdate<SomeDynamoDbEntity>(null!));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void DeleteAsync_NullKey_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() => transactor.DeleteAsync<SomeDynamoDbEntity>(null!, "value"));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void DeleteAsync_NullKeyValue_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() => transactor.DeleteAsync<SomeDynamoDbEntity>("key", null!));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void DeleteAsync_SingleParam_NullKeyValue_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() => transactor.DeleteAsync<SomeDynamoDbEntity>(null!));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void PatchAsync_NullModel_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() => transactor.PatchAsync<SomeDynamoDbEntity>(null!, "Status"));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void PatchAsync_NullPropertyName_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() => transactor.PatchAsync(new SomeDynamoDbEntity { Id = "1" }, null!));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void PatchAsync_Expression_NullKeyValue_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() =>
                transactor.PatchAsync<SomeDynamoDbEntity, string>(null!, x => x.Status, "Active"));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void DeleteAsync_Expression_NullKeyValue_ThrowsArgumentNullException()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);

            Assert.Throws<ArgumentNullException>(() =>
                transactor.DeleteAsync<SomeDynamoDbEntity, string>(x => x.Id, null!));
            Assert.True(transactor.ErrorDuringExecution);
        }

        [Fact]
        public async Task ConditionGreaterThan_AddsToTransaction()
        {
            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                transactor.ConditionGreaterThan<SomeDynamoDbEntity, double>("key1", x => x.Amount, 50.0);
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 1),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ConditionLessThan_AddsToTransaction()
        {
            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                transactor.ConditionLessThan<SomeDynamoDbEntity, double>("key1", x => x.Amount, 200.0);
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 1),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ConditionNotEquals_AddsToTransaction()
        {
            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                transactor.ConditionNotEquals<SomeDynamoDbEntity, string>("key1", x => x.Status, "Deleted");
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 1),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ConditionVersionEquals_AddsToTransaction()
        {
            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                transactor.ConditionVersionEquals<SomeDynamoDbEntity>("key1", x => x.Version, 3L);
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => r.Count() == 1),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public void Constructor_WithIAmazonDynamoDB_CreatesTransactionManager()
        {
            var mockClient = new Mock<IAmazonDynamoDB>();

            var transactor = new DynamoDbTransactor(mockClient.Object);

            Assert.NotNull(transactor);
            Assert.False(transactor.ErrorDuringExecution);
        }

        [Fact]
        public void AddRawRequest_ValidRequest_AddsToRequests()
        {
            var transactor = new DynamoDbTransactor(_mockManager.Object);
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>(
                new KeyValue { Key = "Id", Value = "123" });

            transactor.AddRawRequest(request);

            Assert.False(transactor.ErrorDuringExecution);
        }

        [Fact]
        public async Task EmptyTransaction_StillCallsExecute()
        {
            await using (var transactor = new DynamoDbTransactor(_mockManager.Object))
            {
                // No operations added
            }

            _mockManager.Verify(
                m => m.ExecuteTransactionAsync(
                    It.Is<IEnumerable<ITransactionRequest>>(r => !r.Any()),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }
    }
}
