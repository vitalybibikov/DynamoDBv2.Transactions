using Xunit;
using Moq;
using DynamoDBv2.Transactions;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests.Setup;

public class DynamoDbTransactorTests
{
    private readonly Mock<ITransactionManager> _mockManager;
    private DynamoDbTransactor _transactor;

    public DynamoDbTransactorTests()
    {
        _mockManager = new Mock<ITransactionManager>();
    }

    [Fact]
    public async Task CreateOrUpdate_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        var item = new { Id = 1, Name = "Test" };
        var managerMock = new Mock<ITransactionManager>();
        var transactor = new Mock<DynamoDbTransactor>(managerMock.Object) { CallBase = true };

        transactor.Setup(t => t.AddRawRequest(It.IsAny<ITransactionRequest>()))
            .Throws(new Exception());

        // Act & Assert
        await Assert.ThrowsAsync<Exception>(async () =>
        {
            await using (transactor.Object)
            {
                transactor.Object.CreateOrUpdate(item);
            }
        });

        Assert.True(transactor.Object.ErrorDuringExecution);
    }


    [Fact]
    public void PatchAsync_FirstOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        var model = new SomeDynamoDbEntity { Id = "1" };
        var transactor = new Mock<DynamoDbTransactor>(_mockManager.Object) { CallBase = true };
        transactor.Setup(t => t.AddRawRequest(It.IsAny<ITransactionRequest>())).Throws(new Exception());

        // Act & Assert
        Assert.Throws<Exception>(() => transactor.Object.PatchAsync(model, "Id"));
        Assert.True(transactor.Object.ErrorDuringExecution);
    }


    [Fact]
    public void PatchAsync_SecondOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        var transactor = new Mock<DynamoDbTransactor>(_mockManager.Object) { CallBase = true };
        transactor.Setup(t => t.AddRawRequest(It.IsAny<ITransactionRequest>())).Throws(new Exception());

        // Act & Assert
        Assert.Throws<Exception>(() => transactor.Object.PatchAsync<SomeDynamoDbEntity, double>("", item => item.Amount, 123));

        Assert.True(transactor.Object.ErrorDuringExecution);
    }


    [Fact]
    public void DeleteAsync_FirstOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        var transactor = new Mock<DynamoDbTransactor>(_mockManager.Object) { CallBase = true };
        transactor.Setup(t => t.AddRawRequest(It.IsAny<ITransactionRequest>())).Throws(new Exception());

        // Act & Assert
        Assert.Throws<Exception>(() => transactor.Object.DeleteAsync<SomeDynamoDbEntity>(nameof(SomeDynamoDbEntity.Id), "value"));
        Assert.True(transactor.Object.ErrorDuringExecution);
    }


    [Fact]
    public void DeleteAsync_SecondOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        var transactor = new Mock<DynamoDbTransactor>(_mockManager.Object) { CallBase = true };
        transactor.Setup(t => t.AddRawRequest(It.IsAny<ITransactionRequest>())).Throws(new Exception());

        // Act & Assert
        Assert.Throws<Exception>(() => transactor.Object.DeleteAsync<SomeDynamoDbEntity, string>(test => test.Id, "deletedValue"));
        Assert.True(transactor.Object.ErrorDuringExecution);
    }

    [Fact]
    public async Task DisposeAsync_ErrorDuringExecutionTrue_DoesNotCallExecuteTransaction()
    {
        // Arrange
        var transactor = new Mock<DynamoDbTransactor>(_mockManager.Object) { CallBase = true };
        transactor.Setup(t => t.AddRawRequest(It.IsAny<ITransactionRequest>())).Throws(new Exception());
        try { transactor.Object.CreateOrUpdate(new { }); } catch (Exception) { }

        // Act
        await transactor.Object.DisposeAsync();

        // Assert
        _mockManager.Verify(m => m.ExecuteTransactionAsync(It.IsAny<List<ITransactionRequest>>(), CancellationToken.None), Times.Never);
    }

    [Fact]
    public async Task DisposeAsync_ErrorDuringExecutionFalse_CallsExecuteTransactionOnce()
    {
        // Arrange
        var transactor = new Mock<DynamoDbTransactor>(_mockManager.Object) { CallBase = true };

        // Act
        await transactor.Object.DisposeAsync();

        // Assert
        _mockManager.Verify(m => m.ExecuteTransactionAsync(It.IsAny<List<ITransactionRequest>>(), CancellationToken.None), Times.Once);
    }
}