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

            Assert.Equal("#p0 = :v0", expression);
        }

        [Fact]
        public void ConditionEquals_TwoConditions_NoDuplication()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("123");

            request.Equals<SomeDynamoDbEntity, string>(x => x.Status, "Active");
            request.Equals<SomeDynamoDbEntity, double>(x => x.Amount, 100.0);

            var operation = request.GetOperation();
            var expression = operation.ConditionCheckType!.ConditionExpression;

            // Before fix: expression would contain duplicated conditions when same-named properties used
            // After fix: numbered tokens ensure uniqueness
            Assert.Equal("#p0 = :v0 AND #p1 = :v1", expression);
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
            Assert.Contains("#p0 = :v0", expression);
            Assert.Contains("#p1 > :v1", expression);
            Assert.Contains("#p2 < :v2", expression);
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
            Assert.Equal("#p0 = :v0", expression);
            // The name token should map to "MyId" (the attributed name)
            Assert.Equal("MyId", operation.ConditionCheckType.ExpressionAttributeNames["#p0"]);
        }

        [Fact]
        public void ConditionExpression_SingleCondition_NoTrailingAND()
        {
            var request = new ConditionCheckTransactionRequest<SomeDynamoDbEntity>("key1");

            request.NotEquals<SomeDynamoDbEntity, string>(x => x.Status, "Deleted");

            var operation = request.GetOperation();
            var expression = operation.ConditionCheckType!.ConditionExpression;

            Assert.DoesNotContain(" AND", expression);
            Assert.Equal("#p0 <> :v0", expression);
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

        #region GSI attribute AmbiguousMatchException fix

        /// <summary>
        /// Regression test: GetPropertyAttributedName used GetCustomAttribute (singular) for
        /// DynamoDBPropertyAttribute base class. When a property has both [DynamoDBProperty] and
        /// a GSI attribute (both inherit from DynamoDBPropertyAttribute), the call threw
        /// AmbiguousMatchException. Fix: use GetCustomAttributes (plural) with exact type filter.
        /// </summary>
        [Fact]
        public void GetPropertyAttributedName_WithGsiAndPropertyAttributes_DoesNotThrow()
        {
            // CreatedTimeUtcString has both [DynamoDBGlobalSecondaryIndexRangeKey] and [DynamoDBProperty]
            var name = DynamoDbMapper.GetPropertyAttributedName(typeof(GsiTestEntity), "CreatedTimeUtcString");
            Assert.Equal("CreatedTimeUtcString", name);
        }

        [Fact]
        public void GetPropertyAttributedName_WithRangeKeyAndGsiHashKey_DoesNotThrow()
        {
            // PlayerId has both [DynamoDBRangeKey] and [DynamoDBGlobalSecondaryIndexHashKey]
            var name = DynamoDbMapper.GetPropertyAttributedName(typeof(GsiTestEntity), "PlayerId");
            Assert.Equal("PlayerId", name);
        }

        [Fact]
        public void MapToAttribute_GsiEntity_DoesNotThrow()
        {
            var entity = new GsiTestEntity
            {
                BucketId = "bucket-1",
                PlayerId = "player-1",
                Position = 2,
                CreatedTimeUtcString = "2026/04/09 10:00:00.000",
                WasClaimed = true,
                TTL = 1861075835.0,
                Version = 1
            };

            var attributes = DynamoDbMapper.MapToAttribute(entity);

            Assert.NotEmpty(attributes);
            Assert.Equal("bucket-1", attributes["BucketId"].S);
            Assert.Equal("player-1", attributes["PlayerId"].S);
            Assert.Equal("2026/04/09 10:00:00.000", attributes["CreatedTimeUtcString"].S);
            Assert.True(attributes["WasClaimed"].BOOL);
        }

        [Fact]
        public void PutTransactionRequest_GsiEntity_DoesNotThrowAmbiguousMatch()
        {
            var entity = new GsiTestEntity
            {
                BucketId = "bucket-1",
                PlayerId = "player-1",
                Position = 2,
                CreatedTimeUtcString = "2026/04/09 10:00:00.000",
                WasClaimed = true,
                TTL = 1861075835.0,
                Version = 1
            };

            // This is the exact call path that threw AmbiguousMatchException before the fix:
            // DynamoDbTransactor.CreateOrUpdate -> PutTransactionRequest -> MapToAttribute -> GetPropertyAttributedName
            var request = new PutTransactionRequest<GsiTestEntity>(entity);
            var operation = request.GetOperation();

            Assert.NotNull(operation.PutType);
            Assert.Equal("GsiTestEntity", operation.PutType.TableName);
            Assert.NotEmpty(operation.PutType.Item);
        }

        [Fact]
        public void MapToAttribute_EmptyStringProperty_SerializedAsNull()
        {
            var entity = new GsiTestEntity
            {
                BucketId = "bucket-1",
                PlayerId = "player-1",
                Position = 2,
                CreatedTimeUtcString = "",  // empty string
                WasClaimed = true,
                TTL = 100.0
            };

            var attributes = DynamoDbMapper.MapToAttribute(entity);

            // Empty strings must be serialized as NULL, not as { S = "" }
            // DynamoDB rejects empty AttributeValue: "Supplied AttributeValue is empty"
            Assert.True(attributes["CreatedTimeUtcString"].NULL);
            Assert.Null(attributes["CreatedTimeUtcString"].S);
        }

        [Fact]
        public void MapToAttribute_EmptyAttributeValues_SkippedFromOutput()
        {
            // Comprehensive test: any property that would produce an empty AttributeValue
            // (no S, N, BOOL, NULL, M, L, etc. set) must be excluded from the output
            var entity = new GsiTestEntity
            {
                BucketId = "bucket-1",
                PlayerId = "player-1",
                Position = 0,
                CreatedTimeUtcString = null!,
                WasClaimed = false,
                TTL = 0
            };

            var attributes = DynamoDbMapper.MapToAttribute(entity);

            // Every value in the map must have at least one type discriminator set
            foreach (var kv in attributes)
            {
                var v = kv.Value;
                bool hasType = v.S != null || v.N != null || v.IsBOOLSet || v.NULL == true
                    || v.B != null || (v.M != null && v.M.Count > 0)
                    || (v.L != null && v.L.Count > 0) || (v.SS != null && v.SS.Count > 0)
                    || (v.NS != null && v.NS.Count > 0) || (v.BS != null && v.BS.Count > 0);
                Assert.True(hasType, $"Property '{kv.Key}' produced an empty AttributeValue");
            }
        }

        [Fact]
        public void MapToAttribute_NullStringProperty_NotIncludedInMap()
        {
            var entity = new GsiTestEntity
            {
                BucketId = "bucket-1",
                PlayerId = "player-1",
                Position = 2,
                CreatedTimeUtcString = null!,
                WasClaimed = false,
                TTL = 100.0
            };

            var attributes = DynamoDbMapper.MapToAttribute(entity);

            // Null values should not be in the attribute map
            Assert.False(attributes.ContainsKey("CreatedTimeUtcString"));
        }

        #endregion
    }
}
