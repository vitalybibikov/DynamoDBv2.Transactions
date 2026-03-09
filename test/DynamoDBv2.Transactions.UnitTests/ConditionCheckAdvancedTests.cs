using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class ConditionCheckAdvancedTests
    {
        [Fact]
        public void Equals_SetsCorrectExpressionAndAttributes()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");

            var op = request.GetOperation();
            Assert.Equal("#p0 = :v0", op.ConditionCheckType!.ConditionExpression);
            Assert.Equal("Status", op.ConditionCheckType.ExpressionAttributeNames["#p0"]);
            Assert.Equal("Active", op.ConditionCheckType.ExpressionAttributeValues[":v0"].S);
        }

        [Fact]
        public void NotEquals_SetsCorrectOperator()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.NotEquals<SomeDynamoDbEntity, string>(x => x.Status, "Deleted");

            var op = request.GetOperation();
            Assert.Equal("#p0 <> :v0", op.ConditionCheckType!.ConditionExpression);
        }

        [Fact]
        public void GreaterThan_SetsCorrectOperator()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);

            var op = request.GetOperation();
            Assert.Equal("#p0 > :v0", op.ConditionCheckType!.ConditionExpression);
            Assert.Equal("100", op.ConditionCheckType.ExpressionAttributeValues[":v0"].N);
        }

        [Fact]
        public void LessThan_SetsCorrectOperator()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.LessThan<SomeDynamoDbEntity, double>(x => x.Amount, 50.0);

            var op = request.GetOperation();
            Assert.Equal("#p0 < :v0", op.ConditionCheckType!.ConditionExpression);
        }

        [Fact]
        public void VersionEquals_WithValue_SetsNumericCondition()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.VersionEquals<SomeDynamoDbEntity>(x => x.Version, 5L);

            var op = request.GetOperation();
            Assert.Equal("#p0 = :v0", op.ConditionCheckType!.ConditionExpression);
            Assert.Equal("5", op.ConditionCheckType.ExpressionAttributeValues[":v0"].N);
        }

        [Fact]
        public void VersionEquals_WithNull_SetsNullCondition()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.VersionEquals<SomeDynamoDbEntity>(x => x.Version, null);

            var op = request.GetOperation();
            Assert.Equal("#p0 = :v0", op.ConditionCheckType!.ConditionExpression);
            Assert.True(op.ConditionCheckType.ExpressionAttributeValues[":v0"].NULL);
        }

        [Fact]
        public void Constructor_SetsKeyFromHashKeyAttribute()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("testKey");

            // SomeDynamoDbEntity has [DynamoDBHashKey(AttributeName = "MyId")]
            Assert.Contains("MyId", request.Key.Keys);
            Assert.Equal("testKey", request.Key["MyId"].S);
        }

        [Fact]
        public void Constructor_SimpleHashKeyEntity_SetsKeyFromPropertyName()
        {
            var request = new ConditionCheckTransactionRequest<SimpleHashKeyEntity>("testKey");

            Assert.Contains("Id", request.Key.Keys);
            Assert.Equal("testKey", request.Key["Id"].S);
        }

        [Fact]
        public void GetOperation_NoConditions_ReturnsEmptyConditionExpression()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            var op = request.GetOperation();

            Assert.Null(op.ConditionCheckType!.ConditionExpression);
        }

        [Fact]
        public void GetOperation_WithExpressionAttributeNames_SetsThemOnCheck()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");
            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");
            request.Equals<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);

            var op = request.GetOperation();

            Assert.Equal(2, op.ConditionCheckType!.ExpressionAttributeNames.Count);
            Assert.Equal(2, op.ConditionCheckType.ExpressionAttributeValues.Count);
        }

        [Fact]
        public void ConditionCheck_UsesCustomAttributeName_ForProperty()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key");

            // Name has [DynamoDBProperty(AttributeName = "Name")] — same as property name
            // Id has [DynamoDBHashKey(AttributeName = "MyId")]
            request.Equals<SomeDynamoDbEntity, string>(x => x.Id, "val");

            var op = request.GetOperation();
            Assert.Contains("#p0", op.ConditionCheckType!.ExpressionAttributeNames.Keys);
            Assert.Equal("MyId", op.ConditionCheckType.ExpressionAttributeNames["#p0"]);
        }

        [Fact]
        public void MixedOperators_CorrectCombination()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");
            request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 0.0);

            var op = request.GetOperation();
            var expr = op.ConditionCheckType!.ConditionExpression!;

            Assert.Contains("=", expr);
            Assert.Contains(">", expr);
            Assert.Contains("AND", expr);
            Assert.DoesNotContain("AND AND", expr);
        }
    }
}
