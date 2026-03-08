using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests that verify bug fixes introduced in the modernization update.
    /// </summary>
    public class BugFixVerificationTests
    {
        #region ConditionExpression double-concatenation fix

        [Fact]
        public void ConditionEquals_SingleCondition_NoduplicationInExpression()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");

            var operation = request.GetOperation();
            var expression = operation.ConditionCheckType!.ConditionExpression;

            Assert.Equal("#Status = :StatusValue", expression);
        }

        [Fact]
        public void ConditionEquals_TwoConditions_NoDuplication()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");
            request.Equals<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);

            var operation = request.GetOperation();
            var expression = operation.ConditionCheckType!.ConditionExpression;

            // Before fix: expression would contain duplicated conditions
            // After fix: clean concatenation with AND separator, trailing AND trimmed
            Assert.Equal("#Status = :StatusValue AND #Amount = :AmountValue", expression);
        }

        [Fact]
        public void ConditionCheck_ThreeConditions_CorrectExpression()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");
            request.GreaterThan<SomeDynamoDbEntity, double>(x => x.Amount, 50.0);
            request.LessThan<SomeDynamoDbEntity, double>(x => x.Amount, 200.0);

            var operation = request.GetOperation();
            var expression = operation.ConditionCheckType!.ConditionExpression;

            // Should have exactly 3 conditions joined by AND, no trailing AND
            Assert.Contains("#Status = :StatusValue", expression);
            Assert.Contains("#Amount > :AmountValue", expression);
            Assert.DoesNotContain("AND AND", expression);
            Assert.False(expression!.EndsWith("AND "));
            Assert.False(expression.EndsWith("AND"));
        }

        #endregion

        #region TrimEnd fix - no longer trims chars from condition values

        [Fact]
        public void ConditionExpression_WithPropertyEndingInD_NotCorrupted()
        {
            // "AND" chars should not be trimmed from property names or values
            // The old TrimEnd(' ', 'A', 'N', 'D') would corrupt expressions
            // ending in those characters
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.Equals<SomeDynamoDbEntity, string>(x => x.Id, "testD");

            var operation = request.GetOperation();
            var expression = operation.ConditionCheckType!.ConditionExpression;

            // Expression should be clean - just the condition, no trailing AND
            Assert.Equal("#MyId = :MyIdValue", expression);
        }

        [Fact]
        public void ConditionExpression_SingleCondition_NoTrailingAND()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key1");

            request.NotEquals<SomeDynamoDbEntity, string>(x => x.Status, "Deleted");

            var operation = request.GetOperation();
            var expression = operation.ConditionCheckType!.ConditionExpression;

            Assert.DoesNotContain(" AND", expression);
            Assert.Equal("#Status <> :StatusValue", expression);
        }

        #endregion

        #region GetHashKeyAttributeName fix - returns property name when AttributeName is empty

        [Fact]
        public void GetHashKeyAttributeName_NoAttributeName_ReturnsPropertyName()
        {
            // SimpleHashKeyEntity has [DynamoDBHashKey] without AttributeName
            // Before fix: returned null. After fix: returns "Id" (property name)
            var result = DynamoDbMapper.GetHashKeyAttributeName(typeof(SimpleHashKeyEntity));

            Assert.Equal("Id", result);
        }

        [Fact]
        public void GetHashKeyAttributeName_WithAttributeName_ReturnsAttributeName()
        {
            // SomeDynamoDbEntity has [DynamoDBHashKey(AttributeName = "MyId")]
            var result = DynamoDbMapper.GetHashKeyAttributeName(typeof(SomeDynamoDbEntity));

            Assert.Equal("MyId", result);
        }

        [Fact]
        public void DeleteByKeyValue_SimpleHashKeyEntity_UsesPropertyName()
        {
            // SimpleHashKeyEntity has [DynamoDBHashKey] without custom AttributeName
            var request = new DeleteTransactionRequest<SimpleHashKeyEntity>("value123");

            Assert.Contains("Id", request.Key.Keys);
            Assert.Equal("value123", request.Key["Id"].S);
        }

        [Fact]
        public void PatchByKeyValue_SimpleHashKeyEntity_UsesPropertyName()
        {
            var request = new PatchTransactionRequest<SimpleHashKeyEntity>(
                "value123",
                new Property { Name = "Name", Value = "Updated" });

            Assert.Contains("Id", request.Key.Keys);
        }

        [Fact]
        public void DeleteByKeyValue_AttributedEntity_UsesAttributeName()
        {
            // SomeDynamoDbEntity has [DynamoDBHashKey(AttributeName = "MyId")]
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("value123");

            Assert.Contains("MyId", request.Key.Keys);
            Assert.Equal("value123", request.Key["MyId"].S);
        }

        #endregion

        #region Reflection caching correctness

        [Fact]
        public void ReflectionCache_ConsistentResults_AcrossMultipleCalls()
        {
            // Call multiple times to exercise the cache
            var name1 = DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Id");
            var name2 = DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Id");
            var name3 = DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Id");

            Assert.Equal("MyId", name1);
            Assert.Equal(name1, name2);
            Assert.Equal(name2, name3);
        }

        [Fact]
        public void ReflectionCache_DifferentTypes_ReturnsCorrectResults()
        {
            var attributed = DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Id");
            var notAttributed = DynamoDbMapper.GetPropertyAttributedName(typeof(SomeNotAttributedDynamoDbEntity), "Id");

            Assert.Equal("MyId", attributed);
            Assert.Equal("Id", notAttributed);
        }

        [Fact]
        public void GetVersion_CacheConsistency_SameResultsAcrossCalls()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Version = 5 };

            var (prop1, val1) = DynamoDbMapper.GetVersion(entity);
            var (prop2, val2) = DynamoDbMapper.GetVersion(entity);

            Assert.Equal(prop1, prop2);
            Assert.Equal(val1, val2);
            Assert.Equal("Version", prop1);
            Assert.Equal(5L, val1);
        }

        #endregion
    }
}
