using Amazon.DynamoDBv2.Model;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class DynamoDbMapperFullCoverageTests
    {
        // -------------------
        // -------- V1 --------
        // -------------------

        [Fact]
        public void V1_NullableInt_WithValue_ShouldBeN()
        {
            int? value = 42;
            var result = InvokeV1(value);
            Assert.Equal("42", result.N);
        }

        [Fact]
        public void V1_NullableInt_WithoutValue_ShouldThrow()
        {
            int? value = null;
            var result = InvokeV1(value);
            Assert.True(result.NULL);
        }

        [Fact]
        public void V1_EmptyStringArray_ShouldReturnEmptySS()
        {
            var value = Array.Empty<string>();
            var result = InvokeV1(value);
            Assert.Empty(result.SS);
        }

        [Fact]
        public void V1_EmptyNumericArray_ShouldReturnEmptyNS()
        {
            var value = Array.Empty<int>();
            var result = InvokeV1(value);
            Assert.Empty(result.NS);
        }

        [Fact]
        public void V1_SingleChar_ShouldBeS()
        {
            char value = 'Z';
            var result = InvokeV1(value);
            Assert.Equal("Z", result.S);
        }

        [Fact]
        public void V1_List_OfDateTimes_ShouldBeSS()
        {
            var now = DateTime.UtcNow;
            var value = new List<DateTime> { now };
            var result = InvokeV1(value);
            Assert.Equal(now.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), result.SS.First());
        }

        [Fact]
        public void V1_NestedObject_ShouldBeM()
        {
            var nested = new { Foo = "Bar", Amount = 123 };
            var result = InvokeV1(nested);
            Assert.Equal("Bar", result.M["Foo"].S);
            Assert.Equal("123", result.M["Amount"].N);
        }

        [Fact]
        public void V1_ListOfBinaries_ShouldBeBS()
        {
            var value = new List<byte[]>
        {
            new byte[] { 0x01 },
            new byte[] { 0x02, 0x03 }
        };
            var result = InvokeV1(value);
            Assert.Equal(2, result.BS.Count);
        }

        [Fact]
        public void V1_EmptyHashSet_ShouldBeEmptySet()
        {
            var value = new HashSet<int>();
            var result = InvokeV1(value);
            Assert.Empty(result.NS);
        }

        [Fact]
        public void V1_HashSetOfDateTimes_ShouldBeSS()
        {
            var value = new HashSet<DateTime> { DateTime.UtcNow };
            var result = InvokeV1(value);
            Assert.Single(result.SS);
        }

        // -------------------
        // -------- V2 --------
        // -------------------

        [Fact]
        public void V2_NullableDecimal_WithValue_ShouldBeN()
        {
            decimal? value = 99.99m;
            var result = InvokeV2(value);
            Assert.Equal("99.99", result.N);
        }

        [Fact]
        public void V2_NullableDecimal_WithoutValue_ShouldBeNull()
        {
            decimal? value = null;
            var result = InvokeV2(value);
            Assert.True(result.NULL);
        }

        [Fact]
        public void V2_ListOfBooleans_ShouldBeL()
        {
            var value = new List<bool> { true, false };
            var result = InvokeV2(value);
            Assert.True(result.L[0].BOOL);
            Assert.False(result.L[1].BOOL);
        }

        [Fact]
        public void V2_ArrayOfGuids_ShouldBeL()
        {
            var guid = Guid.NewGuid();
            var value = new[] { guid };
            var result = InvokeV2(value);
            Assert.Equal(guid.ToString(), result.L[0].S);
        }

        [Fact]
        public void V2_ListOfMemoryStreams_ShouldBeL()
        {
            var value = new List<MemoryStream>
        {
            new MemoryStream(new byte[] { 0x01 }),
            new MemoryStream(new byte[] { 0x02 })
        };
            var result = InvokeV2(value);
            Assert.Equal(2, result.L.Count);
            Assert.NotNull(result.L[0].B);
        }

        [Fact]
        public void V2_HashSetOfGuids_ShouldBeSS()
        {
            var value = new HashSet<Guid> { Guid.NewGuid() };
            var result = InvokeV2(value);
            Assert.Single(result.SS);
        }

        [Fact]
        public void V2_HashSetOfChars_ShouldBeSS()
        {
            var value = new HashSet<char> { 'A', 'B' };
            var result = InvokeV2(value);
            Assert.Equal(new List<string> { "A", "B" }, result.SS);
        }

        [Fact]
        public void V2_List_OfNestedObjects_ShouldBeL()
        {
            var value = new[]
            {
            new { A = "1", B = 2 },
            new { A = "2", B = 3 }
        };
            var result = InvokeV2(value);
            Assert.Equal(2, result.L.Count);
            Assert.Equal("1", result.L[0].M["A"].S);
            Assert.Equal("3", result.L[1].M["B"].N);
        }

        [Fact]
        public void V2_Dictionary_Mapping()
        {
            var dict = new Dictionary<string, object>
        {
            { "key1", "value" },
            { "key2", 123 }
        };
            var result = InvokeV2(dict);
            Assert.Equal("value", result.M["key1"].S);
            Assert.Equal("123", result.M["key2"].N);
        }

        [Fact]
        public void V2_EmptyList_ShouldBeEmptyL()
        {
            var value = new List<int>();
            var result = InvokeV2(value);
            Assert.Empty(result.L);
        }

        [Fact]
        public void V2_NestedDictionaries()
        {
            var value = new Dictionary<string, object>
        {
            { "parent", new Dictionary<string, object> { { "child", 123 } } }
        };
            var result = InvokeV2(value);
            Assert.Equal("123", result.M["parent"].M["child"].N);
        }

        // -------------------
        // Helpers for private converters
        // -------------------
        private AttributeValue InvokeV1(object value) => typeof(DynamoDbMapper)
            .GetMethod("ConvertToAttributeValueV1", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?.Invoke(null, new[] { value }) as AttributeValue;

        private AttributeValue InvokeV2(object value) => typeof(DynamoDbMapper)
            .GetMethod("ConvertToAttributeValueV2", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?.Invoke(null, new[] { value }) as AttributeValue;
    }

}
