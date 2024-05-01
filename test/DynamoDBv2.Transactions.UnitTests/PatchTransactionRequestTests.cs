using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class PatchTransactionRequestTests
    {
        [Fact]
        public void Constructor_WithKeyValueAndProperty_InitializesCorrectly()
        {
            // Arrange
            var keyValue = new KeyValue { Key = "Id", Value = "123" };
            var property = new Property { Name = "Status", Value = "Active" };

            // Act
            var request = new PatchTransactionRequest<SomeDynamoDbEntity>(keyValue, property);

            // Assert
            Assert.Equal("SET #Property = :newValue", request.UpdateExpression);
            Assert.Contains("#Property", request.ExpressionAttributeNames.Keys);
            Assert.Contains(":newValue", request.ExpressionAttributeValues.Keys);
        }

        [Fact]
        public void GetOperation_ReturnsCorrectUpdateOperation()
        {
            // Arrange
            var keyValue = new KeyValue { Key = "Id", Value = "123" };
            var property = new Property { Name = "Status", Value = "Active" };
            var request = new PatchTransactionRequest<SomeDynamoDbEntity>(keyValue, property);

            // Act
            var operation = request.GetOperation();

            // Assert
            Assert.NotNull(operation);
            Assert.IsType<Update>(operation.UpdateType);
            var update = operation.UpdateType;
            Assert.Equal(nameof(SomeDynamoDbEntity), update.TableName);
            Assert.Equal("SET #Property = :newValue", update.UpdateExpression);
        }
    }

}
