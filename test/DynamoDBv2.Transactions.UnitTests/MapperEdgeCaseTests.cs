using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Additional edge case tests for DynamoDbMapper.
    /// </summary>
    public class MapperEdgeCaseTests
    {
        #region MapToAttribute edge cases

        [Fact]
        public void MapToAttribute_NullObject_ReturnsEmpty()
        {
            var result = DynamoDbMapper.MapToAttribute(null);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void MapToAttribute_EmptyStringProperty_MapsAsString()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = "", Status = "" };

            var result = DynamoDbMapper.MapToAttribute(entity);

            Assert.Equal("", result["Name"].S);
            Assert.Equal("", result["Status"].S);
        }

        [Fact]
        public void MapToAttribute_V1Conversion_UsesBoolAsNumber()
        {
            var obj = new SimpleEntity { Flag = true, Name = "test" };

            var result = DynamoDbMapper.MapToAttribute(obj, DynamoDBEntryConversion.V1);

            Assert.Equal("1", result["Flag"].N);
        }

        [Fact]
        public void MapToAttribute_V2Conversion_UsesBoolAsBool()
        {
            var obj = new SimpleEntity { Flag = true, Name = "test" };

            var result = DynamoDbMapper.MapToAttribute(obj, DynamoDBEntryConversion.V2);

            Assert.True(result["Flag"].BOOL);
        }

        [Fact]
        public void MapToAttribute_NullPropertyValues_Skipped()
        {
            var entity = new SomeDynamoDbEntity { Id = "1", Name = null!, Status = null! };

            var result = DynamoDbMapper.MapToAttribute(entity);

            // Non-null: MyId, Amount (default 0.0), Version (null => NULL attr)
            Assert.Contains("MyId", result.Keys);
            Assert.Contains("Amount", result.Keys);
            Assert.DoesNotContain("Name", result.Keys);
            Assert.DoesNotContain("Status", result.Keys);
        }

        #endregion

        #region GetPropertyAttributedName edge cases

        [Fact]
        public void GetPropertyAttributedName_NonExistentProperty_Throws()
        {
            Assert.Throws<ArgumentException>(() =>
                DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "NonExistent"));
        }

        [Fact]
        public void GetPropertyAttributedName_PropertyWithoutAttribute_ReturnsPropertyName()
        {
            var result = DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Status");

            Assert.Equal("Status", result);
        }

        #endregion

        #region GetHashKeyAttributeName edge cases

        [Fact]
        public void GetHashKeyAttributeName_NoHashKey_Throws()
        {
            Assert.Throws<ArgumentException>(() =>
                DynamoDbMapper.GetHashKeyAttributeName(typeof(NoHashKeyEntity)));
        }

        #endregion

        #region GetAttributeValue type coverage

        [Fact]
        public void GetAttributeValue_String_ReturnsS()
        {
            var result = DynamoDbMapper.GetAttributeValue("hello");
            Assert.Equal("hello", result!.S);
        }

        [Fact]
        public void GetAttributeValue_Int_ReturnsN()
        {
            var result = DynamoDbMapper.GetAttributeValue(42);
            Assert.Equal("42", result!.N);
        }

        [Fact]
        public void GetAttributeValue_Bool_ReturnsBOOL()
        {
            var result = DynamoDbMapper.GetAttributeValue(true);
            Assert.True(result!.BOOL);
        }

        [Fact]
        public void GetAttributeValue_DateTime_ReturnsISOString()
        {
            var dt = new DateTime(2026, 3, 8, 12, 0, 0, DateTimeKind.Utc);
            var result = DynamoDbMapper.GetAttributeValue(dt);
            Assert.Equal("2026-03-08T12:00:00.000Z", result!.S);
        }

        [Fact]
        public void GetAttributeValue_Guid_ReturnsString()
        {
            var guid = Guid.Parse("12345678-1234-1234-1234-123456789012");
            var result = DynamoDbMapper.GetAttributeValue(guid);
            Assert.Equal("12345678-1234-1234-1234-123456789012", result!.S);
        }

        [Fact]
        public void GetAttributeValue_Decimal_ReturnsN()
        {
            var result = DynamoDbMapper.GetAttributeValue(123.456m);
            Assert.Equal("123.456", result!.N);
        }

        [Fact]
        public void GetAttributeValue_ByteArray_ReturnsB()
        {
            var bytes = new byte[] { 1, 2, 3 };
            var result = DynamoDbMapper.GetAttributeValue(bytes);
            Assert.NotNull(result!.B);
        }

        [Fact]
        public void GetAttributeValue_Char_ReturnsS()
        {
            var result = DynamoDbMapper.GetAttributeValue('X');
            Assert.Equal("X", result!.S);
        }

        [Fact]
        public void GetAttributeValue_Long_ReturnsN()
        {
            var result = DynamoDbMapper.GetAttributeValue(long.MaxValue);
            Assert.Equal(long.MaxValue.ToString(), result!.N);
        }

        [Fact]
        public void GetAttributeValue_Float_ReturnsN()
        {
            var result = DynamoDbMapper.GetAttributeValue(3.14f);
            Assert.Equal("3.14", result!.N);
        }

        [Fact]
        public void GetAttributeValue_Double_ReturnsN()
        {
            var result = DynamoDbMapper.GetAttributeValue(2.718281828);
            Assert.Equal("2.718281828", result!.N);
        }

        [Fact]
        public void GetAttributeValue_MemoryStream_WithNonZeroPosition_RewindsAndReturnsB()
        {
            var ms = new MemoryStream(new byte[] { 10, 20, 30 });
            ms.ReadByte(); // Advance position

            var result = DynamoDbMapper.GetAttributeValue(ms);

            Assert.NotNull(result!.B);
            Assert.Equal(0, result.B.Position);
        }

        #endregion

        #region Dictionary mapping

        [Fact]
        public void MapToAttribute_DictionaryProperty_MapsCorrectly()
        {
            var obj = new EntityWithDict
            {
                Id = "1",
                Data = new Dictionary<string, string>
                {
                    { "key1", "val1" },
                    { "key2", "val2" }
                }
            };

            var result = DynamoDbMapper.MapToAttribute(obj);

            Assert.NotNull(result["Data"].M);
            Assert.Equal("val1", result["Data"].M["key1"].S);
            Assert.Equal("val2", result["Data"].M["key2"].S);
        }

        #endregion

        #region Thread safety of caches

        [Fact]
        public void ConcurrentAccess_ToPropertyCache_NoCrash()
        {
            var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
            {
                DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Id");
                DynamoDbMapper.GetPropertyAttributedName(typeof(SomeDynamoDbEntity), "Name");
                DynamoDbMapper.GetHashKeyAttributeName(typeof(SomeDynamoDbEntity));
                DynamoDbMapper.GetVersion(new SomeDynamoDbEntity { Id = "1", Version = 1 });
            }));

            Task.WaitAll(tasks.ToArray());
        }

        #endregion

        #region Test helper entities

        private class SimpleEntity
        {
            [DynamoDBHashKey]
            public string Id { get; set; } = "default";
            public bool Flag { get; set; }
            public string Name { get; set; } = "";
        }

        private class NoHashKeyEntity
        {
            public string SomeProperty { get; set; } = "";
        }

        private class EntityWithDict
        {
            [DynamoDBHashKey]
            public string Id { get; set; } = "";
            public Dictionary<string, string> Data { get; set; } = new();
        }

        #endregion
    }
}
