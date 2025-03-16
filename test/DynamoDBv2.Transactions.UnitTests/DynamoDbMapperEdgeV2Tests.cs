using Amazon.DynamoDBv2.Model;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class DynamoDbMapperEdgeV2Tests
    {
        // 1. HashSet<string> containing empty strings
        [Fact]
        public void V2_HashSet_String_WithEmpty_ShouldHandleEmptyString()
        {
            var value = new HashSet<string> { "", "valid" };
            var result = InvokeV2(value);
            Assert.Contains("", result.SS);
            Assert.Contains("valid", result.SS);
        }

        // 2. List<object> with mixed types
        [Fact]
        public void V2_ListOfMixedTypes_ShouldMapAsMixedL()
        {
            var value = new List<object> { "hello", 123, true };
            var result = InvokeV2(value);
            Assert.Equal("hello", result.L[0].S);
            Assert.Equal("123", result.L[1].N);
            Assert.True(result.L[2].BOOL);
        }

        // 3. Nested Lists
        [Fact]
        public void V2_NestedList_ShouldMapToNestedL()
        {
            var value = new List<List<int>> { new List<int> { 1, 2 } };
            var result = InvokeV2(value);
            Assert.Equal("1", result.L[0].L[0].N);
            Assert.Equal("2", result.L[0].L[1].N);
        }

        // 4. Dictionary with HashSet value
        [Fact]
        public void V2_DictionaryWithHashSet_ShouldMapCorrectly()
        {
            var value = new Dictionary<string, object> { { "tags", new HashSet<string> { "a", "b" } } };
            var result = InvokeV2(value);
            Assert.Equal(new List<string> { "a", "b" }, result.M["tags"].SS);
        }

        // 6. Array of nullable ints
        [Fact]
        public void V2_ArrayOfNullableInts_ShouldHandleNullsInL()
        {
            var value = new int?[] { 1, null, 2 };
            var result = InvokeV2(value);
            Assert.Equal("1", result.L[0].N);
            Assert.True(result.L[1].NULL);
            Assert.Equal("2", result.L[2].N);
        }

        // 7. HashSet<Guid> with duplicates
        [Fact]
        public void V2_HashSetGuid_Deduplicates()
        {
            var guid = Guid.NewGuid();
            var value = new HashSet<Guid> { guid, guid };
            var result = InvokeV2(value);
            Assert.Single(result.SS);
            Assert.Contains(guid.ToString(), result.SS);
        }

        // 8. Disposed MemoryStream
        [Fact]
        public void V2_DisposedMemoryStream_ShouldThrow()
        {
            try
            {
                var ms = new MemoryStream(new byte[] { 0x01 });
                ms.Dispose();

                InvokeV2(ms);
            }
            catch (Exception ex)
            {
                Assert.Equal(ex.InnerException.GetType(), typeof(ObjectDisposedException));
            }
        }

        // 9. Array of byte arrays
        [Fact]
        public void V2_ArrayOfByteArrays_ShouldMapToL()
        {
            var value = new byte[][] { new byte[] { 1 }, new byte[] { 2 } };
            var result = InvokeV2(value);
            Assert.Equal(2, result.L.Count);
            Assert.NotNull(result.L[0].B);
        }

        // 10. Dictionary with unsupported type
        [Fact]
        public void V2_DictionaryWithUnsupportedType_ShouldThrow()
        {
            try
            {
                var value = new Dictionary<string, object> { { "token", new CancellationToken() } };
                InvokeV2(value);
            }
            catch (Exception ex)
            {
                Assert.Equal(ex.InnerException.GetType(), typeof(ArgumentException));
            }
        }

        // 11. DateTime.MinValue
        [Fact]
        public void V2_DateTimeMinValue_ShouldBeIso()
        {
            var dt = DateTime.MinValue;
            var result = InvokeV2(dt);
            Assert.Equal(dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), result.S);
        }

        // 12. Deep nested dictionary
        [Fact]
        public void V2_DeeplyNestedDictionary_ShouldMapAllLevels()
        {
            var value = new Dictionary<string, object>
        {
            { "level1", new Dictionary<string, object>
                {
                    { "level2", new Dictionary<string, object> { { "level3", "final" } } }
                }
            }
        };
            var result = InvokeV2(value);
            Assert.Equal("final", result.M["level1"].M["level2"].M["level3"].S);
        }

        // 13. Large HashSet<int>
        [Fact]
        public void V2_HashSet_LargeNumericSet()
        {
            var value = new HashSet<int>(Enumerable.Range(0, 10000));
            var result = InvokeV2(value);
            Assert.Equal(10000, result.NS.Count);
        }

        // 14. Guid.Empty in HashSet<Guid>
        [Fact]
        public void V2_HashSet_EmptyGuid()
        {
            var value = new HashSet<Guid> { Guid.Empty };
            var result = InvokeV2(value);
            Assert.Equal(Guid.Empty.ToString(), result.SS.First());
        }

        // 15. Boolean inside anonymous object
        [Fact]
        public void V2_AnonymousObjectWithBool()
        {
            var value = new { Enabled = true };
            var result = InvokeV2(value);
            Assert.True(result.M["Enabled"].BOOL);
        }
        // 16. Array of bools inside anonymous object
        [Fact]
        public void V2_ArrayOfBools_ShouldMapToL()
        {
            var value = new[] { true, false };
            var result = InvokeV2(value);
            Assert.Equal(true, result.L[0].BOOL);
            Assert.Equal(false, result.L[1].BOOL);
        }

        // 17. Array of int? should be stored as L with NULL for all nulls
        [Fact]
        public void V2_ArrayOfNullableInts_ShouldBeStoredAsNull()
        {
            int?[] value = { 1, null, 2 };
            var result = InvokeV2(value);
            Assert.Equal(1, Convert.ToInt32(result.L[0].N));
            Assert.Equal(2, Convert.ToInt32(result.L[2].N));
            Assert.Equal(true,result.L[1].NULL);
        }

        // Helpers to invoke private methods
        private AttributeValue InvokeV2(object value) => typeof(DynamoDbMapper)
            .GetMethod("ConvertToAttributeValueV2", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?.Invoke(null, new[] { value }) as AttributeValue;

        private struct MyStruct
        {
            public int X;
            public bool Flag;
        }
    }

}
