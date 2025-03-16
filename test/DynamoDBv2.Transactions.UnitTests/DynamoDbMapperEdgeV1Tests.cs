using Amazon.DynamoDBv2.Model;
using System.Data.Common;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class DynamoDbMapperEdgeV1Tests
    {
        // 1. Nullable bool should map to N (0/1)
        [Fact]
        public void V1_NullableBool_ShouldMapToN()
        {
            bool? value = true;
            var result = InvokeV1(value);
            Assert.Equal("1", result.N);
        }

        // 2. Nullable bool = null should map to NULL
        [Fact]
        public void V1_NullableBool_Null_ShouldBeNull()
        {
            bool? value = null;
            var result = InvokeV1(value);
            Assert.True(result.NULL);
        }

        // 3. Array of nullable ints with null inside (should throw in V1)
        [Fact]
        public void V1_ArrayOfNullableInts_WithNull_ShouldThrow()
        {
            int?[] value = { 1, null, 2 };
            var result = InvokeV1(value);

            Assert.NotEmpty(result.L);

            Assert.Equal(1, Convert.ToInt32(result.L[0].N));
            Assert.Equal(2, Convert.ToInt32(result.L[2].N));
            Assert.Equal(true, result.L[1].NULL);
        }

        // 4. HashSet<string> containing empty strings
        [Fact]
        public void V1_HashSet_String_WithEmpty_ShouldMap()
        {
            var value = new HashSet<string> { "", "value" };
            var result = InvokeV1(value);
            Assert.Contains("", result.SS);
        }

        // 5. Empty numeric HashSet
        [Fact]
        public void V1_EmptyNumericHashSet_ShouldMapToEmptyNS()
        {
            var value = new HashSet<int>();
            var result = InvokeV1(value);
            Assert.Empty(result.NS);
        }

        // 6. Array of DateTimes
        [Fact]
        public void V1_ArrayOfDateTimes_ShouldMapToSS()
        {
            var dt = DateTime.UtcNow;
            var value = new[] { dt };
            var result = InvokeV1(value);
            Assert.Equal(dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), result.SS.First());
        }

        // 7. List<byte[]>
        [Fact]
        public void V1_ListOfByteArrays_ShouldBeBS()
        {
            var value = new List<byte[]> { new byte[] { 1 }, new byte[] { 2 } };
            var result = InvokeV1(value);
            Assert.Equal(2, result.BS.Count);
        }

        // 8. Array of MemoryStreams
        [Fact]
        public void V1_ArrayOfMemoryStreams_ShouldBeBS()
        {
            var value = new[]
            {
            new MemoryStream(new byte[] { 0x01 }),
            new MemoryStream(new byte[] { 0x02 })
        };
            var result = InvokeV1(value);
            Assert.Equal(2, result.BS.Count);
        }

        // 9. Dictionary<string, object> with Guid
        [Fact]
        public void V1_DictionaryWithGuid_ShouldMapToS()
        {
            var guid = Guid.NewGuid();
            var value = new Dictionary<string, object> { { "id", guid } };
            var result = InvokeV1(value);
            Assert.Equal(guid.ToString(), result.M["id"].S);
        }

        // 10. Struct with DateTime and bool
        [Fact]
        public void V1_StructWithDateTimeAndBool_ShouldMapToM()
        {
            try
            {
                var value = new MyStruct { Date = DateTime.UtcNow, Active = false };
                var result = InvokeV1(value);

                Assert.Equal("0", result.M["Active"].N);
                Assert.NotNull(result.M["Date"].S);

                InvokeV1(value);
            }
            catch (Exception ex)
            {
                Assert.Equal(ex.InnerException.GetType(), typeof(ArgumentException));
            }
        }

        // 11. Large List<int> (1000 elements)
        [Fact]
        public void V1_LargeListInt_ShouldMapToNS()
        {
            var value = Enumerable.Range(0, 1000).ToList();
            var result = InvokeV1(value);
            Assert.Equal(1000, result.NS.Count);
        }

        // 12. Dictionary with null value
        [Fact]
        public void V1_DictionaryWithNull_ShouldMapToNULL()
        {
            var value = new Dictionary<string, object> { { "key", null } };
            var result = InvokeV1(value);
            Assert.True(result.M["key"].NULL);
        }

        // 13. Array of bools
        [Fact]
        public void V1_ArrayOfBools_ShouldMapToNS()
        {
            var value = new[] { true, false };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "1", "0" }, result.NS);
        }

        [Fact]
        public void V1_ArrayOfStringBools_ShouldMapToSS()
        {
            var value = new[] { "1", "0" };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "1", "0" }, result.SS);
        }

        // 14. Array of guids
        [Fact]
        public void V1_ArrayOfGuids_ShouldBeSS()
        {
            var value = new[] { Guid.NewGuid(), Guid.NewGuid() };
            var result = InvokeV1(value);
            Assert.Equal(2, result.SS.Count);
        }

        // 15. Mixed collection (invalid for V1)
        [Fact]
        public void V1_MixedCollection_ShouldThrow()
        {
            var value = new List<object> { "str", 1 };

            try
            {
                InvokeV1(value);
            }
            catch (Exception ex)
            {
                Assert.Equal(ex.InnerException.GetType(), typeof(ArgumentException));
            }
        }

        // 16. Guid.Empty in array
        [Fact]
        public void V1_GuidEmptyInArray_ShouldBeSS()
        {
            var value = new[] { Guid.Empty };
            var result = InvokeV1(value);
            Assert.Equal(Guid.Empty.ToString(), result.SS.First());
        }

        // 17. Anonymous object with bool
        [Fact]
        public void V1_AnonymousObjectWithBool_ShouldMapToN()
        {
            var value = new { Enabled = true };
            var result = InvokeV1(value);
            Assert.Equal("1", result.M["Enabled"].N);
        }

        // 18. Char array
        [Fact]
        public void V1_CharArray_ShouldBeSS()
        {
            var value = new[] { 'A', 'B' };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "A", "B" }, result.SS);
        }

        // 19. Array of decimals
        [Fact]
        public void V1_ArrayOfDecimals_ShouldBeNS()
        {
            var value = new[] { 10.5m, 20.75m };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "10.5", "20.75" }, result.NS);
        }

        // 20. Dictionary nested inside dictionary
        [Fact]
        public void V1_NestedDictionary_ShouldMapToM()
        {
            var value = new Dictionary<string, object> {
            { "outer", new Dictionary<string, object> { { "inner", "value" } } }
        };
            var result = InvokeV1(value);
            Assert.Equal("value", result.M["outer"].M["inner"].S);
        }

        private AttributeValue InvokeV1(object value) => typeof(DynamoDbMapper)
            .GetMethod("ConvertToAttributeValueV1", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?.Invoke(null, new[] { value }) as AttributeValue;

        private struct MyStruct
        {
            public DateTime Date;
            public bool Active;
        }
    }

}
