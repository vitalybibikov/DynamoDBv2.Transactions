using System;
using Xunit;
using Moq;
using System.Threading.Tasks;
using DynamoDBv2.Transactions;
using DynamoDBv2.Transactions.Contracts;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.UnitTests;
using DynamoDBv2.Transactions.UnitTests.Setup;

public class DynamoDbTransactorTests
{
    private readonly Mock<ITransactionManager> _mockManager;
    private DynamoDbTransactor _transactor;

    public DynamoDbTransactorTests()
    {
        _mockManager = new Mock<ITransactionManager>();
        _transactor = new DynamoDbTransactor(_mockManager.Object);
    }

    [Fact]
    public void CreateOrUpdate_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        var item = new { Id = 1, Name = "Test" };
        SimulateError(); // Simulate exception in transaction execution

        // Act & Assert
        Assert.Throws<Exception>(() => _transactor.CreateOrUpdate(item));
        Assert.True(_transactor.ErrorDuringExecution);
    }


    [Fact]
    public void PatchAsync_FirstOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        var model = new { Id = 1 };
        SimulateError();

        // Act & Assert
        Assert.Throws<Exception>(() => _transactor.PatchAsync(model, "Id"));
        Assert.True(_transactor.ErrorDuringExecution);
    }

    [Fact]
    public void PatchAsync_SecondOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        SimulateError();

        // Act & Assert
        Assert.Throws<Exception>(() => _transactor.PatchAsync<SomeDynamoDbEntity, string>("1", item => item.Id, "NewValue"));
        Assert.True(_transactor.ErrorDuringExecution);
    }

    [Fact]
    public void DeleteAsync_FirstOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        SimulateError();

        // Act & Assert
        Assert.Throws<Exception>(() => _transactor.DeleteAsync<string>("key", "value"));
        Assert.True(_transactor.ErrorDuringExecution);
    }

    [Fact]
    public void DeleteAsync_SecondOverload_ThrowsException_SetsErrorDuringExecutionTrue()
    {
        // Arrange
        SimulateError();
        var test = new SomeDynamoDbEntity();
        ;
        // Act & Assert
        Assert.Throws<Exception>(() => _transactor.DeleteAsync<SomeDynamoDbEntity, string>(test => test.Id, "deletedValue"));
        Assert.True(_transactor.ErrorDuringExecution);
    }

    [Fact]
    public async Task DisposeAsync_ErrorDuringExecutionTrue_DoesNotCallExecuteTransaction()
    {
        // Arrange
        // Simulating an error scenario
        _transactor.CreateOrUpdate(new { }); // Assuming this will throw and set ErrorDuringExecution to true
        _mockManager.Setup(m => m.ExecuteTransactionAsync(It.IsAny<List<ITransactionRequest>>(), CancellationToken.None))
            .Throws(new Exception());

        // Act
        await _transactor.DisposeAsync();

        // Assert
        _mockManager.Verify(m => m.ExecuteTransactionAsync(It.IsAny<List<ITransactionRequest>>(), CancellationToken.None), Times.Never);
    }

    [Fact]
    public async Task DisposeAsync_ErrorDuringExecutionFalse_CallsExecuteTransactionOnce()
    {
        // Arrange
        // Assuming no error has occurred

        // Act
        await _transactor.DisposeAsync();

        // Assert
        _mockManager.Verify(m => m.ExecuteTransactionAsync(It.IsAny<List<ITransactionRequest>>(), CancellationToken.None), Times.Once);
    }


    private void SimulateError()
    {
        _mockManager.Setup(m => m.ExecuteTransactionAsync(It.IsAny<List<ITransactionRequest>>(), CancellationToken.None))
            .Throws(new Exception());
    }
}