namespace DynamoDBv2.Transactions.UnitTests
{
    using Amazon.DynamoDBv2.Model;
    using DynamoDBv2.Transactions;
    using DynamoDBv2.Transactions.Requests.Abstract;
    using Xunit;

    public class OperationTests
    {
        [Fact]
        public void DeleteOperation_InitializesCorrectTypeAndProperty()
        {
            // Arrange
            var delete = new Delete();

            // Act
            var operation = Operation.Delete(delete);

            // Assert
            Assert.Equal(TransactOperationType.Delete, operation.Type);
            Assert.Equal(delete, operation.DeleteType);
        }

        [Fact]
        public void PutOperation_InitializesCorrectTypeAndProperty()
        {
            // Arrange
            var put = new Put();

            // Act
            var operation = Operation.Put(put);

            // Assert
            Assert.Equal(TransactOperationType.Put, operation.Type);
            Assert.Equal(put, operation.PutType);
        }

        [Fact]
        public void UpdateOperation_InitializesCorrectTypeAndProperty()
        {
            // Arrange
            var update = new Update();

            // Act
            var operation = Operation.Update(update);

            // Assert
            Assert.Equal(TransactOperationType.Update, operation.Type);
            Assert.Equal(update, operation.UpdateType);
        }

        [Fact]
        public void PatchOperation_InitializesCorrectTypeAndProperty()
        {
            // Arrange
            var update = new Update();

            // Act
            var operation = Operation.Patch(update);

            // Assert
            Assert.Equal(TransactOperationType.Patch, operation.Type);
            Assert.Equal(update, operation.UpdateType);
        }

        [Fact]
        public void ConditionCheckOperation_InitializesCorrectTypeAndProperty()
        {
            // Arrange
            var check = new ConditionCheck();

            // Act
            var operation = Operation.Check(check);

            // Assert
            Assert.Equal(TransactOperationType.ConditionCheck, operation.Type);
            Assert.Equal(check, operation.ConditionCheckType);
        }
    }

}
