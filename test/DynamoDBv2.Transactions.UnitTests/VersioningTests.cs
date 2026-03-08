using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests for version tracking and optimistic concurrency control.
    /// </summary>
    public class VersioningTests
    {
        [Fact]
        public void PutRequest_NullVersion_SetsVersionToZero()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Version = null };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            // Version should be initialized to 0 for new items
            Assert.Equal("0", request.PutRequest.Item["Version"].N);
        }

        [Fact]
        public void PutRequest_ExistingVersion_IncrementsVersion()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Version = 5 };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            // Version should be incremented from 5 to 6
            Assert.Equal("6", request.PutRequest.Item["Version"].N);
        }

        [Fact]
        public void PutRequest_ExistingVersion_SetsConditionExpression()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Version = 3 };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            Assert.Equal("#Version = :expectedVersion", request.ConditionExpression);
            Assert.Equal("Version", request.ExpressionAttributeNames["#Version"]);
            Assert.Equal("3", request.ExpressionAttributeValues[":expectedVersion"].N);
        }

        [Fact]
        public void PutRequest_NullVersion_NoConditionExpression()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Version = null };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            Assert.Null(request.ConditionExpression);
            Assert.Empty(request.ExpressionAttributeNames);
            Assert.Empty(request.ExpressionAttributeValues);
        }

        [Fact]
        public void PutRequest_VersionZero_IncrementsToOne()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Version = 0 };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            Assert.Equal("1", request.PutRequest.Item["Version"].N);
            Assert.Equal("#Version = :expectedVersion", request.ConditionExpression);
            Assert.Equal("0", request.ExpressionAttributeValues[":expectedVersion"].N);
        }

        [Fact]
        public void PutRequest_EntityWithoutVersion_NoVersionHandling()
        {
            var entity = new SomeNotAttributedDynamoDbEntity { Id = "1", Name = "Test" };

            var request = new PutTransactionRequest<SomeNotAttributedDynamoDbEntity>(entity);

            // SomeNotAttributedDynamoDbEntity has Version property but no [DynamoDBVersion] attribute
            Assert.Null(request.ConditionExpression);
        }

        [Fact]
        public void GetOperation_WithVersion_IncludesConditionInPut()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Version = 10 };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);
            var operation = request.GetOperation();
            var put = operation.PutType!;

            Assert.Equal("#Version = :expectedVersion", put.ConditionExpression);
            Assert.NotEmpty(put.ExpressionAttributeNames);
            Assert.NotEmpty(put.ExpressionAttributeValues);
        }

        [Fact]
        public void GetVersion_ReturnsCorrectPropertyNameAndValue()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Version = 42 };

            var (propertyName, value) = DynamoDbMapper.GetVersion(entity);

            Assert.Equal("Version", propertyName);
            Assert.Equal(42L, value);
        }

        [Fact]
        public void GetVersion_NullEntity_ReturnsNulls()
        {
            var (propertyName, value) = DynamoDbMapper.GetVersion<SomeDynamoDbEntity>(null);

            Assert.Null(propertyName);
            Assert.Null(value);
        }

        [Fact]
        public void GetVersion_EntityWithNullVersion_ReturnsPropertyNameWithNullValue()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Version = null };

            var (propertyName, value) = DynamoDbMapper.GetVersion(entity);

            Assert.Equal("Version", propertyName);
            Assert.Null(value);
        }
    }
}
