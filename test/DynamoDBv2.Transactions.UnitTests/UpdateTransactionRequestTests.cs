using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class UpdateTransactionRequestTests
    {
        [Fact]
        public void Constructor_WithExpression_SetsUpdateExpression()
        {
            // Arrange
            var expression = "SET #Status = :status";

            // Act
            var request = new UpdateTransactionRequest<SomeDynamoDbEntity>(expression);

            // Assert
            Assert.Equal(expression, request.UpdateExpression);
        }

        [Fact]
        public void GetOperation_ReturnsCorrectUpdateOperation()
        {
            // Arrange
            var expression = "SET #Status = :status";
            var request = new UpdateTransactionRequest<SomeDynamoDbEntity>(expression);

            // Act
            var operation = request.GetOperation();

            // Assert
            Assert.NotNull(operation);
            Assert.IsType<Update>(operation.UpdateType);
            var update = operation.UpdateType;
            Assert.Equal(nameof(SomeDynamoDbEntity), update.TableName);
            Assert.Equal(expression, update.UpdateExpression);
        }

        // Additional tests...
    }

}
