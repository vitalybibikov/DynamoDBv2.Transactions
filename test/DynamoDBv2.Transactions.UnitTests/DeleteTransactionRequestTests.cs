using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class DeleteTransactionRequestTests
    {
        [Fact]
        public void Constructor_WithKeyValue_SetsKeyCorrectly()
        {
            // Arrange
            var keyValue = new KeyValue { Key = "Id", Value = "123" };

            // Act
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>(keyValue);

            // Assert
            Assert.Contains("MyId", request.Key.Keys);
            Assert.Equal("123", request.Key["MyId"].S);
        }

        [Fact]
        public void GetOperation_ReturnsCorrectDeleteOperation()
        {
            // Arrange
            var key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "123" } } };
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>(key);

            // Act
            var operation = request.GetOperation();

            // Assert
            Assert.NotNull(operation);
            Assert.IsType<Delete>(operation.DeleteType);
            var delete = operation.DeleteType;
            Assert.Equal(nameof(SomeDynamoDbEntity), delete.TableName);  // Assuming TableName is set in the constructor
            Assert.Equal(key, delete.Key);
        }

        // Additional tests...
    }

}
