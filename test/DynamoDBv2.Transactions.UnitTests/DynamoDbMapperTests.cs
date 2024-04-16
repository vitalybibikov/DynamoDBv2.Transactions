using Amazon.DynamoDBv2.DataModel;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class DynamoDbMapperTests
    {
        [Fact]
        public void GetPropertyAttributedName_WithHashKey_ReturnsCorrectName()
        {
            // Arrange
            var type = typeof(MyTestClass);
            var propertyName = "Id";

            // Act
            var result = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("MyId", result); // Assuming "MyId" is the custom attribute name set in MyTestClass
        }

        [Fact]
        public void GetAttributeValue_WithNullValue_ReturnsNullAttribute()
        {
            // Arrange
            object value = null;

            // Act
            var result = DynamoDbMapper.GetAttributeValue(value);

            // Assert
            Assert.True(result.NULL);
        }

        [Fact]
        public void MapToAttribute_WithComplexObject_ReturnsCorrectDictionary()
        {
            // Arrange
            var obj = new MyTestClass { Id = "123", Name = "Test", Amount = 10.5 };

            // Act
            var result = DynamoDbMapper.MapToAttribute(obj);

            // Assert
            Assert.Contains("Name", result.Keys);
            Assert.Equal("Test", result["Name"].S);
            Assert.Contains("Amount", result.Keys);
            Assert.Equal("10.5", result["Amount"].N);
        }

        [Fact]
        public void GetHashKeyAttributeName_NoHashKey_ThrowsException()
        {
            // Arrange
            var type = typeof(MyTestClassWithoutHashKey);

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => DynamoDbMapper.GetHashKeyAttributeName(type));
            Assert.Contains("Failed to find hash key attribute on type", exception.Message);
        }

        [Fact]
        public void GetHashKeyAttributeName_ValidHashKey_ReturnsCustomAttributeName()
        {
            // Arrange
            Type type = typeof(MyTestClass);

            // Act
            var result = DynamoDbMapper.GetHashKeyAttributeName(type);

            // Assert
            Assert.Equal("MyId", result);
        }


        [Fact]
        public void GetVersion_WithVersionAttribute_ReturnsCorrectVersion()
        {
            // Arrange
            var item = new VersionedClass { Name = "Test", Version = 1 };

            // Act
            var (propertyName, value) = DynamoDbMapper.GetVersion(item);

            // Assert
            Assert.Equal("Version", propertyName);
            Assert.Equal(1, value);
        }

        [Fact]
        public void GetVersion_WithoutVersionAttribute_ReturnsNull()
        {
            // Arrange
            var item = new NonVersionedClass { Name = "Test" };

            // Act
            var (propertyName, value) = DynamoDbMapper.GetVersion(item);

            // Assert
            Assert.Null(propertyName);
            Assert.Null(value);
        }

        [Fact]
        public void GetVersion_NullObject_ReturnsNull()
        {
            // Arrange
            VersionedClass item = null;

            // Act
            var (propertyName, value) = DynamoDbMapper.GetVersion(item);

            // Assert
            Assert.Null(propertyName);
            Assert.Null(value);
        }

        [Fact]
        public void GetVersion_WithNullVersionField_ReturnsNullValue()
        {
            // Arrange
            var item = new VersionedClass { Name = "Test", Version = null };

            // Act
            var (propertyName, value) = DynamoDbMapper.GetVersion(item);

            // Assert
            Assert.Equal("Version", propertyName);
            Assert.Null(value);
        }

        [Fact]
        public void MapToAttribute_ValidObject_MapsAllProperties()
        {
            // Arrange
            var obj = new SimpleClass { Name = "John", Age = 30 };

            // Act
            var result = DynamoDbMapper.MapToAttribute(obj);

            // Assert
            Assert.Equal("John", result["Name"].S);
            Assert.Equal("30", result["Age"].N);
        }

        [Fact]
        public void MapToAttribute_NullObject_ReturnsEmptyDictionary()
        {
            // Arrange
            object obj = null;

            // Act
            var result = DynamoDbMapper.MapToAttribute(obj);

            // Assert
            Assert.Empty(result);
        }

        [Fact]
        public void MapToAttribute_IgnoreNonReadableProperties_DoesNotIncludeThem()
        {
            // Arrange
            var obj = new SimpleClass { Name = "John", Age = 30 };

            // Modify the class definition for this test to include a write-only property.
            // Act
            var result = DynamoDbMapper.MapToAttribute(obj);

            // Assert
            Assert.Contains("Name", result.Keys);
            Assert.DoesNotContain("WriteOnlyProperty", result.Keys);
        }


        [Fact]
        public void MapToAttribute_CompleteEntity_MapsAllPropertiesCorrectly()
        {
            // Arrange
            var testEntity = new TestEntity
            {
                Name = "John Doe",
                Age = 30,
                AccountBalance = 12345.67m,
                IsActive = true,
                LastLogin = new DateTime(2023, 4, 5, 14, 30, 00, DateTimeKind.Utc),
                Scores = new List<int> { 95, 82, 88 },
                Preferences = new Dictionary<string, string> { { "theme", "dark" }, { "language", "English" } },
                Nested = new TestNestedEntity
                {
                    Description = "Level 2",
                    Level = 2
                }
            };

            // Act
            var result = DynamoDbMapper.MapToAttribute(testEntity);

            // Assert
            Assert.Equal("John Doe", result["Name"].S);
            Assert.Equal("30", result["Age"].N);
            Assert.Equal("12345.67", result["AccountBalance"].N);
            Assert.True(result["IsActive"].BOOL);
            Assert.Equal("2023-04-05T14:30:00.000Z", result["LastLogin"].S);
            Assert.Equal(3, result["Scores"].L.Count);
            Assert.Equal("95", result["Scores"].L[0].N);
            Assert.Equal("dark", result["Preferences"].M["theme"].S);
            Assert.Equal("Level 2", result["Nested"].M["Description"].S);
            Assert.Equal("2", result["Nested"].M["Level"].N);
        }



        [Fact]
        public void GetPropertyAttributedName_ReturnsHashKeyAttributeName()
        {
            // Arrange
            Type type = typeof(TestItem);
            string propertyName = "Id";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("Id", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ReturnsRangeKeyAttributeName()
        {
            // Arrange
            Type type = typeof(TestItem);
            string propertyName = "OrderNumber";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("OrderNumber", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ReturnsPropertyAttributeName()
        {
            // Arrange
            Type type = typeof(TestItem);
            string propertyName = "Description";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("Description", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ReturnsPropertyName_WhenNoAttributeDefined()
        {
            // Arrange
            Type type = typeof(TestItem);
            string propertyName = "Price";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("Price", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ThrowsArgumentException_WhenPropertyNotFound()
        {
            // Arrange
            Type type = typeof(TestItem);
            string propertyName = "NonExistentProperty";

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => DynamoDbMapper.GetPropertyAttributedName(type, propertyName));
            Assert.Contains(propertyName, exception.Message);
        }

        [Fact]
        public void GetPropertyAttributedName_ReturnsCustomHashKeyAttributeName()
        {
            // Arrange
            Type type = typeof(ItemWithCustomAttributes);
            string propertyName = "ItemId";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("PrimaryKey", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ReturnsCustomRangeKeyAttributeName()
        {
            // Arrange
            Type type = typeof(ItemWithCustomAttributes);
            string propertyName = "SerialNumber";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("RangeKey", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ReturnsCustomPropertyAttributeName()
        {
            // Arrange
            Type type = typeof(ItemWithCustomAttributes);
            string propertyName = "Description";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("ItemDescription", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ReturnsPropertyName_WhenNoAttributeDefined1()
        {
            // Arrange
            Type type = typeof(ItemWithCustomAttributes);
            string propertyName = "Price";

            // Act
            string attributeName = DynamoDbMapper.GetPropertyAttributedName(type, propertyName);

            // Assert
            Assert.Equal("Price", attributeName);
        }

        [Fact]
        public void GetPropertyAttributedName_ThrowsArgumentException_WhenPropertyNotFound1()
        {
            // Arrange
            Type type = typeof(ItemWithCustomAttributes);
            string propertyName = "NonExistent";

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(() => DynamoDbMapper.GetPropertyAttributedName(type, propertyName));
            Assert.Contains(propertyName, exception.Message);
            Assert.Contains("not found", exception.Message);
        }
    }

    internal class ItemWithCustomAttributes
    {
        [DynamoDBHashKey("PrimaryKey")]
        public int ItemId { get; set; }

        [DynamoDBRangeKey("RangeKey")]
        public string SerialNumber { get; set; }

        [DynamoDBProperty("ItemDescription")]
        public string Description { get; set; }

        [DynamoDBVersion("VersionNumber")]
        public int? Version { get; set; }

        public double Price { get; set; }
    }

    internal class TestItem
    {
        [DynamoDBHashKey("Id")]
        public int Id { get; set; }

        [DynamoDBRangeKey("OrderNumber")]
        public string OrderNumber { get; set; }

        [DynamoDBProperty("Description")]
        public string Description { get; set; }

        public double Price { get; set; }
    }

    internal class TestEntity
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public decimal AccountBalance { get; set; }
        public bool IsActive { get; set; }
        public DateTime LastLogin { get; set; }
        public IList<int> Scores { get; set; }
        public IDictionary<string, string> Preferences { get; set; }
        public TestNestedEntity Nested { get; set; }
    }

    internal class TestNestedEntity
    {
        public string Description { get; set; }
        public int Level { get; set; }
    }

    internal class MyTestClass
    {
        [DynamoDBHashKey(AttributeName = "MyId")]
        public string Id { get; set; }

        [DynamoDBProperty(AttributeName = "Name")]
        public string Name { get; set; }

        public double Amount { get; set; }
    }

    internal class MyTestClassWithoutHashKey
    {
        public string Id { get; set; }
    }

    internal class VersionedClass
    {
        public string Name { get; set; }

        [DynamoDBVersion]
        public int? Version { get; set; }
    }

    internal class NonVersionedClass
    {
        public string Name { get; set; }
    }

    internal class SimpleClass
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
