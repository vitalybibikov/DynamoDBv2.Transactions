using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class ConditionCheckTransactionRequestTests
    {
        [Fact]
        public void Constructor_SetsCorrectType()
        {
            // Arrange & Act
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>();

            // Assert
            Assert.Equal(TransactOperationType.ConditionCheck, request.Type);
        }

        [Fact]
        public void GetOperation_ReturnsCorrectOperationDetails()
        {
            // Arrange
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>()
            {
                Key = new Dictionary<string, AttributeValue> { { "Id", new AttributeValue { S = "123" } } },
                ConditionExpression = "attribute_exists(Id)"
            };

            // Act
            var operation = request.GetOperation();

            // Assert
            Assert.NotNull(operation);
            Assert.IsType<ConditionCheck>(operation.ConditionCheckType);
            var conditionCheck = operation.ConditionCheckType;
            Assert.Equal(nameof(SomeDynamoDbEntity), conditionCheck.TableName);
            Assert.Equal("attribute_exists(Id)", conditionCheck.ConditionExpression);
        }
    }
}
