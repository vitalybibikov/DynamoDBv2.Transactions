using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests for fix #8: Properties decorated with [DynamoDBIgnore] must be excluded
    /// from serialization (MapToAttribute) and deserialization (MapFromAttributes).
    /// </summary>
    public class DynamoDBIgnoreTests
    {
        [Fact]
        public void MapToAttribute_IgnoresPropertiesWithDynamoDBIgnore_SourceGen()
        {
            // IgnoreTestEntity is partial → source-generated path
            var entity = new IgnoreTestEntity
            {
                Id = "1",
                Name = "Test",
                SecretField = "secret-value",
                ComputedValue = 99,
                Price = 19.99m
            };

            var result = DynamoDbMapper.MapToAttribute(entity);

            // Ignored properties should NOT appear in the output
            Assert.DoesNotContain("SecretField", result.Keys);
            Assert.DoesNotContain("ComputedValue", result.Keys);
        }

        [Fact]
        public void MapToAttribute_IgnoresPropertiesWithDynamoDBIgnore_Reflection()
        {
            // IgnoreReflectionTestEntity is NOT partial → reflection fallback
            var entity = new IgnoreReflectionTestEntity
            {
                Id = "1",
                Name = "Test",
                InternalState = "should-be-ignored",
                Amount = 100
            };

            var result = DynamoDbMapper.MapToAttribute(entity);

            // Ignored property should NOT appear
            Assert.DoesNotContain("InternalState", result.Keys);
        }

        [Fact]
        public void MapToAttribute_IncludesNonIgnoredProperties()
        {
            // Source-gen path
            var entity = new IgnoreTestEntity
            {
                Id = "1",
                Name = "Visible",
                SecretField = "hidden",
                ComputedValue = 42,
                Price = 9.99m
            };

            var result = DynamoDbMapper.MapToAttribute(entity);

            // Non-ignored properties MUST be present
            Assert.Contains("pk", result.Keys);      // Id → attributed as "pk"
            Assert.Equal("1", result["pk"].S);

            Assert.Contains("Name", result.Keys);
            Assert.Equal("Visible", result["Name"].S);

            Assert.Contains("Price", result.Keys);
            Assert.Equal("9.99", result["Price"].N);
        }

        [Fact]
        public void MapFromAttributes_SkipsIgnoredProperties_SourceGen()
        {
            // Provide attributes that include keys matching ignored property names.
            // The deserialization should not attempt to set them.
            var attributes = new Dictionary<string, AttributeValue>
            {
                { "pk", new AttributeValue { S = "id-1" } },
                { "Name", new AttributeValue { S = "FromDb" } },
                { "SecretField", new AttributeValue { S = "hacked-value" } },
                { "ComputedValue", new AttributeValue { N = "999" } },
                { "Price", new AttributeValue { N = "5.50" } }
            };

            var result = DynamoDbMapper.MapFromAttributes<IgnoreTestEntity>(attributes);

            // Non-ignored properties should be deserialized
            Assert.Equal("id-1", result.Id);
            Assert.Equal("FromDb", result.Name);
            Assert.Equal(5.50m, result.Price);

            // Ignored properties should retain their default values, NOT the DB values
            Assert.Equal("should-not-serialize", result.SecretField);
            Assert.Equal(42, result.ComputedValue);
        }

        [Fact]
        public void MapFromAttributes_SkipsIgnoredProperties_Reflection()
        {
            var attributes = new Dictionary<string, AttributeValue>
            {
                { "pk", new AttributeValue { S = "id-2" } },
                { "Name", new AttributeValue { S = "ReflTest" } },
                { "InternalState", new AttributeValue { S = "hacked" } },
                { "Amount", new AttributeValue { N = "50" } }
            };

            var result = DynamoDbMapper.MapFromAttributes<IgnoreReflectionTestEntity>(attributes);

            Assert.Equal("id-2", result.Id);
            Assert.Equal("ReflTest", result.Name);
            Assert.Equal(50, result.Amount);

            // Ignored property keeps its default
            Assert.Equal("internal", result.InternalState);
        }

        [Fact]
        public void GetCachedProperties_ExcludesIgnoredProperties()
        {
            // We verify this indirectly through MapToAttribute output:
            // If GetCachedProperties correctly excludes ignored properties,
            // then MapToAttribute will never include them regardless of their values.

            // Reflection path
            var reflEntity = new IgnoreReflectionTestEntity
            {
                Id = "test",
                Name = "name",
                InternalState = "state-value",
                Amount = 42
            };

            var reflResult = DynamoDbMapper.MapToAttribute(reflEntity);
            Assert.DoesNotContain("InternalState", reflResult.Keys);

            // Source-gen path
            var genEntity = new IgnoreTestEntity
            {
                Id = "test",
                Name = "name",
                SecretField = "secret",
                ComputedValue = 100,
                Price = 1.0m
            };

            var genResult = DynamoDbMapper.MapToAttribute(genEntity);
            Assert.DoesNotContain("SecretField", genResult.Keys);
            Assert.DoesNotContain("ComputedValue", genResult.Keys);

            // Verify expected count: Id (pk) + Name + Price = 3
            Assert.Equal(3, genResult.Count);
        }
    }
}
