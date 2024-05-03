using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class PutTransactionRequestTests
    {
        [Fact]
        public void Constructor_WithItem_SetsPutRequestCorrectly()
        {
            // Arrange
            var item = new SomeDynamoDbEntity { Id = "123", Name = "Test Item" };

            // Act
            var request = new PutTransactionRequest<SomeDynamoDbEntity>(item);

            // Assert
            Assert.NotNull(request.PutRequest);
            Assert.Equal(nameof(SomeDynamoDbEntity), request.PutRequest.TableName);
            Assert.Contains("MyId", request.PutRequest.Item.Keys);
            Assert.Contains("Name", request.PutRequest.Item.Keys);
        }

        [Fact]
        public void Constructor_WithItem_SetsPutRequestCorrectlyNoAttribute()
        {
            // Arrange
            var item = new SomeNotAttributedDynamoDbEntity { Id = "123", Name = "Test Item" };

            // Act
            var request = new PutTransactionRequest<SomeNotAttributedDynamoDbEntity>(item);

            // Assert
            Assert.NotNull(request.PutRequest);
            Assert.Equal(nameof(SomeNotAttributedDynamoDbEntity), request.PutRequest.TableName);
            Assert.Contains("Id", request.PutRequest.Item.Keys);
            Assert.Contains("Name", request.PutRequest.Item.Keys);
        }

        [Fact]
        public void GetOperation_ReturnsCorrectPutOperation()
        {
            // Arrange
            var item = new SomeDynamoDbEntity { Id = "123", Name = "Test Item" };
            var request = new PutTransactionRequest<SomeDynamoDbEntity>(item);

            // Act
            var operation = request.GetOperation();

            // Assert
            Assert.NotNull(operation);
            Assert.IsType<Put>(operation.PutType);

            var put = operation.PutType;
            Assert.Equal(nameof(SomeDynamoDbEntity), put.TableName);
            Assert.Equal(request.PutRequest.Item, put.Item);
        }
    }
}
