using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests for fix #2: GetVersion should return the DynamoDB attribute name (from
    /// [DynamoDBProperty(AttributeName = "...")] ), not the C# property name, when a
    /// custom attribute name is specified on a version property.
    /// </summary>
    public class VersionAttributeRenameTests
    {
        [Fact]
        public void GetVersion_WithCustomAttributeName_ReturnsAttributeNameNotPropertyName()
        {
            // VersionRenameTestEntity is partial → source-generated path
            // Version property has [DynamoDBProperty(AttributeName = "ver_num")]
            var entity = new VersionRenameTestEntity { Id = "1", Name = "Test", Version = 3 };

            var (propertyName, value) = DynamoDbMapper.GetVersion(entity);

            // Should return the attribute name "ver_num", NOT the C# property name "Version"
            Assert.Equal("ver_num", propertyName);
            Assert.Equal(3L, value);
        }

        [Fact]
        public void GetVersion_WithCustomAttributeName_ReflectionFallback()
        {
            // VersionRenameReflectionEntity is NOT partial → reflection path
            var entity = new VersionRenameReflectionEntity { Id = "1", Name = "Test", Version = 7 };

            var (propertyName, value) = DynamoDbMapper.GetVersion(entity);

            // Reflection path calls GetPropertyAttributedName, which resolves
            // [DynamoDBProperty(AttributeName = "custom_version")]
            Assert.Equal("custom_version", propertyName);
            Assert.Equal(7L, value);
        }

        [Fact]
        public void GetVersion_WithoutCustomAttributeName_ReturnsPropertyName()
        {
            // SomeDynamoDbEntity has [DynamoDBVersion] without custom DynamoDBProperty name
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "Test", Version = 10 };

            var (propertyName, value) = DynamoDbMapper.GetVersion(entity);

            Assert.Equal("Version", propertyName);
            Assert.Equal(10L, value);
        }

        [Fact]
        public void PutTransactionRequest_WithCustomVersion_UsesAttributeNameInCondition()
        {
            // VersionRenameTestEntity (partial / source-gen) has Version=5, custom attr "ver_num"
            var entity = new VersionRenameTestEntity { Id = "1", Name = "Test", Version = 5 };

            var request = new PutTransactionRequest<VersionRenameTestEntity>(entity);

            // SetVersion should use the attribute name "ver_num" in the expression
            Assert.Contains("#Version", request.ExpressionAttributeNames.Keys);
            Assert.Equal("ver_num", request.ExpressionAttributeNames["#Version"]);

            Assert.Contains(":expectedVersion", request.ExpressionAttributeValues.Keys);
            Assert.Equal("5", request.ExpressionAttributeValues[":expectedVersion"].N);

            Assert.Equal("#Version = :expectedVersion", request.ConditionExpression);
        }

        [Fact]
        public void PutTransactionRequest_WithCustomVersion_ReflectionPath()
        {
            // VersionRenameReflectionEntity (non-partial / reflection) has Version=5, custom attr "custom_version"
            var entity = new VersionRenameReflectionEntity { Id = "1", Name = "Test", Version = 5 };

            var request = new PutTransactionRequest<VersionRenameReflectionEntity>(entity);

            // SetVersion should use the attribute name "custom_version" in the expression
            Assert.Contains("#Version", request.ExpressionAttributeNames.Keys);
            Assert.Equal("custom_version", request.ExpressionAttributeNames["#Version"]);

            Assert.Contains(":expectedVersion", request.ExpressionAttributeValues.Keys);
            Assert.Equal("5", request.ExpressionAttributeValues[":expectedVersion"].N);

            Assert.Equal("#Version = :expectedVersion", request.ConditionExpression);
        }

        [Fact]
        public void SetVersion_IncrementsVersionCorrectly_WithCustomName()
        {
            // With Version=5, the Put should increment to 6 in the item dict under "ver_num"
            var entity = new VersionRenameTestEntity { Id = "1", Name = "Test", Version = 5 };

            var request = new PutTransactionRequest<VersionRenameTestEntity>(entity);

            // The version in the serialized item should be incremented to 6
            Assert.Equal("6", request.PutRequest.Item["ver_num"].N);
        }
    }
}
