using Amazon.DynamoDBv2;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// TDD tests verifying that the source-generated mapping produces identical results
    /// to the reflection-based DynamoDbMapper for all key operations.
    /// </summary>
    public class SourceGeneratorMappingTests
    {
        [Fact]
        public void Generated_GetPropertyAttributeName_MatchesReflection_HashKey()
        {
            // [DynamoDBHashKey(AttributeName = "MyId")] string Id
            var generated = SomeDynamoDbEntity.__DynamoDbMetadata.GetPropertyAttributeName("Id");
            var reflected = DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Id");

            // Since we registered the mapping, DynamoDbMapper now uses generated path too.
            // Verify the generated metadata directly returns the correct attribute name.
            Assert.Equal("MyId", generated);
            Assert.Equal(generated, reflected);
        }

        [Fact]
        public void Generated_GetPropertyAttributeName_MatchesReflection_DynamoDBProperty()
        {
            // [DynamoDBProperty(AttributeName = "Name")] string Name
            var generated = SomeDynamoDbEntity.__DynamoDbMetadata.GetPropertyAttributeName("Name");
            Assert.Equal("Name", generated);
        }

        [Fact]
        public void Generated_GetPropertyAttributeName_UnattributedProperty_ReturnsPropertyName()
        {
            // public string Status { get; set; } — no DynamoDB attribute
            var generated = SomeDynamoDbEntity.__DynamoDbMetadata.GetPropertyAttributeName("Status");
            Assert.Equal("Status", generated);
        }

        [Fact]
        public void Generated_GetPropertyAttributeName_UnknownProperty_Throws()
        {
            // Unknown property throws ArgumentException, matching reflection behavior
            Assert.Throws<ArgumentException>(() =>
                SomeDynamoDbEntity.__DynamoDbMetadata.GetPropertyAttributeName("NonExistent"));
        }

        [Fact]
        public void Generated_HashKeyAttributeName_IsCorrect()
        {
            Assert.Equal("MyId", SomeDynamoDbEntity.__DynamoDbMetadata.HashKeyAttributeName);
        }

        [Fact]
        public void Generated_HashKeyAttributeName_MatchesReflection()
        {
            var reflected = DynamoDbMapper.GetHashKeyAttributeName(typeof(SomeDynamoDbEntity));
            Assert.Equal(SomeDynamoDbEntity.__DynamoDbMetadata.HashKeyAttributeName, reflected);
        }

        [Fact]
        public void Generated_TableName_IsCorrect()
        {
            // [DynamoDBTable("SomeDynamoDbEntity")]
            Assert.Equal("SomeDynamoDbEntity", SomeDynamoDbEntity.__DynamoDbMetadata.TableName);
        }

        [Fact]
        public void Generated_TableName_MatchesReflection()
        {
            var reflected = DynamoDbMapper.GetTableName(typeof(SomeDynamoDbEntity));
            Assert.Equal(SomeDynamoDbEntity.__DynamoDbMetadata.TableName, reflected);
        }

        [Fact]
        public void Generated_MapToAttributes_MatchesReflection()
        {
            var entity = new SomeDynamoDbEntity
            {
                Id = "test-id-123",
                Name = "Test Name",
                Status = "Active",
                Amount = 99.95,
                Version = 5
            };

            var generated = SomeDynamoDbEntity.__DynamoDbMetadata.MapToAttributes(entity);
            var reflected = DynamoDbMapper.MapToAttribute(entity, DynamoDBEntryConversion.V2);

            Assert.Equal(reflected.Count, generated.Count);

            foreach (var kvp in reflected)
            {
                Assert.True(generated.ContainsKey(kvp.Key), $"Generated map missing key '{kvp.Key}'");

                var genVal = generated[kvp.Key];
                var refVal = kvp.Value;

                // Compare by attribute value type
                if (refVal.S != null)
                {
                    Assert.Equal(refVal.S, genVal.S);
                }
                else if (refVal.N != null)
                {
                    Assert.Equal(refVal.N, genVal.N);
                }
                else if (refVal.NULL == true)
                {
                    Assert.Equal(true, genVal.NULL);
                }
            }
        }

        [Fact]
        public void Generated_MapToAttributes_NullVersion_SetsNullAttribute()
        {
            var entity = new SomeDynamoDbEntity
            {
                Id = "id-1",
                Name = "Name",
                Status = "Active",
                Amount = 10.0,
                Version = null
            };

            var generated = SomeDynamoDbEntity.__DynamoDbMetadata.MapToAttributes(entity);

            Assert.True(generated.ContainsKey("Version"));
            Assert.Equal(true, generated["Version"].NULL);
        }

        [Fact]
        public void Generated_MapToAttributes_WithVersion_SetsNumericAttribute()
        {
            var entity = new SomeDynamoDbEntity
            {
                Id = "id-1",
                Name = "Name",
                Status = "Active",
                Amount = 10.0,
                Version = 42
            };

            var generated = SomeDynamoDbEntity.__DynamoDbMetadata.MapToAttributes(entity);

            Assert.True(generated.ContainsKey("Version"));
            Assert.Equal("42", generated["Version"].N);
        }

        [Fact]
        public void Generated_GetVersion_ReturnsVersionProperty()
        {
            var entity = new SomeDynamoDbEntity
            {
                Id = "id-1",
                Name = "Name",
                Status = "Active",
                Amount = 10.0,
                Version = 7
            };

            var (versionProp, value) = SomeDynamoDbEntity.__DynamoDbMetadata.GetVersion(entity);

            Assert.Equal("Version", versionProp);
            Assert.Equal(7L, value);
        }

        [Fact]
        public void Generated_GetVersion_NullVersion_ReturnsNullValue()
        {
            var entity = new SomeDynamoDbEntity
            {
                Id = "id-1",
                Name = "Name",
                Status = "Active",
                Amount = 10.0,
                Version = null
            };

            var (versionProp, value) = SomeDynamoDbEntity.__DynamoDbMetadata.GetVersion(entity);

            Assert.Equal("Version", versionProp);
            Assert.Null(value);
        }

        [Fact]
        public void Generated_GetVersion_MatchesReflection()
        {
            var entity = new SomeDynamoDbEntity
            {
                Id = "id-1",
                Name = "Name",
                Status = "Active",
                Amount = 10.0,
                Version = 3
            };

            var (genProp, genVal) = SomeDynamoDbEntity.__DynamoDbMetadata.GetVersion(entity);
            var (refProp, refVal) = DynamoDbMapper.GetVersion(entity);

            Assert.Equal(refProp, genProp);
            Assert.Equal(refVal, genVal);
        }

        [Fact]
        public void DynamoDbMapper_UsesGeneratedMapping_ForRegisteredType()
        {
            // The module initializer should have registered SomeDynamoDbEntity.
            // Verify that DynamoDbMapper.MapToAttribute delegates to generated code
            // by checking the results match exactly.
            var entity = new SomeDynamoDbEntity
            {
                Id = "test-via-mapper",
                Name = "Mapper Test",
                Status = "OK",
                Amount = 123.45,
                Version = 1
            };

            // DynamoDbMapper should use the generated path
            var result = DynamoDbMapper.MapToAttribute(entity);
            Assert.Equal("test-via-mapper", result["MyId"].S);
            Assert.Equal("Mapper Test", result["Name"].S);
            Assert.Equal("OK", result["Status"].S);
            Assert.Equal("1", result["Version"].N);
        }

        [Fact]
        public void DynamoDbMapper_GetTableName_UsesGeneratedMapping()
        {
            var tableName = DynamoDbMapper.GetTableName(typeof(SomeDynamoDbEntity));
            Assert.Equal("SomeDynamoDbEntity", tableName);
        }
    }
}
