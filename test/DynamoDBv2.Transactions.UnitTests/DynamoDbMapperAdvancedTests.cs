using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class DynamoDbMapperAdvancedTests
    {
        // -------- V1 Tests --------

        [Fact]
        public void ConvertToAttributeValueV1_Bool_StoredAsN()
        {
            var value = true;
            var result = InvokeV1(value);
            Assert.Equal("1", result.N);
        }

        [Fact]
        public void ConvertToAttributeValueV1_Guid_StoredAsS()
        {
            var value = Guid.NewGuid();
            var result = InvokeV1(value);
            Assert.Equal(value.ToString(), result.S);
        }

        [Fact]
        public void ConvertToAttributeValueV1_HashSet_Numeric_StoredAsNS()
        {
            var value = new HashSet<int> { 1, 2, 3 };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "1", "2", "3" }, result.NS);
        }

        [Fact]
        public void ConvertToAttributeValueV1_HashSet_String_StoredAsSS()
        {
            var value = new HashSet<string> { "foo", "bar" };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "foo", "bar" }, result.SS);
        }

        [Fact]
        public void ConvertToAttributeValueV1_HashSet_Binary_StoredAsBS()
        {
            var value = new HashSet<byte[]>
        {
            new byte[] { 1, 2 },
            new byte[] { 3, 4 }
        };
            var result = InvokeV1(value);
            Assert.Equal(2, result.BS.Count);
        }

        [Fact]
        public void ConvertToAttributeValueV1_Array_Numeric_StoredAsNS()
        {
            var value = new int[] { 4, 5, 6 };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "4", "5", "6" }, result.NS);
        }

        [Fact]
        public void ConvertToAttributeValueV1_Array_String_StoredAsSS()
        {
            var value = new[] { "A", "B" };
            var result = InvokeV1(value);
            Assert.Equal(new List<string> { "A", "B" }, result.SS);
        }

        [Fact]
        public void ConvertToAttributeValueV1_ByteStoredAsN()
        {
            var value = (byte)42;
            var result = InvokeV1(value);
            Assert.Equal("42", result.N);
        }

        // -------- V2 Tests --------

        [Fact]
        public void ConvertToAttributeValueV2_Bool_StoredAsBOOL()
        {
            var value = true;
            var result = InvokeV2(value);
            Assert.True(result.BOOL);
        }

        [Fact]
        public void ConvertToAttributeValueV2_Guid_StoredAsS()
        {
            var value = Guid.NewGuid();
            var result = InvokeV2(value);
            Assert.Equal(value.ToString(), result.S);
        }

        [Fact]
        public void ConvertToAttributeValueV2_HashSet_Numeric_StoredAsNS()
        {
            var value = new HashSet<int> { 10, 20, 30 };
            var result = InvokeV2(value);
            Assert.Equal(new List<string> { "10", "20", "30" }, result.NS);
        }

        [Fact]
        public void ConvertToAttributeValueV2_HashSet_String_StoredAsSS()
        {
            var value = new HashSet<string> { "alpha", "beta" };
            var result = InvokeV2(value);
            Assert.Equal(new List<string> { "alpha", "beta" }, result.SS);
        }

        [Fact]
        public void ConvertToAttributeValueV2_HashSet_Binary_StoredAsBS()
        {
            var value = new HashSet<byte[]>
        {
            new byte[] { 9, 9 },
            new byte[] { 8, 8 }
        };
            var result = InvokeV2(value);
            Assert.Equal(2, result.BS.Count);
        }

        [Fact]
        public void ConvertToAttributeValueV2_List_Numeric_StoredAsL()
        {
            var value = new List<int> { 100, 200 };
            var result = InvokeV2(value);
            Assert.Equal(2, result.L.Count);
            Assert.Equal("100", result.L[0].N);
            Assert.Equal("200", result.L[1].N);
        }

        [Fact]
        public void ConvertToAttributeValueV2_List_String_StoredAsL()
        {
            var value = new List<string> { "X", "Y" };
            var result = InvokeV2(value);
            Assert.Equal(2, result.L.Count);
            Assert.Equal("X", result.L[0].S);
            Assert.Equal("Y", result.L[1].S);
        }

        [Fact]
        public void ConvertToAttributeValueV2_List_Binary_StoredAsL()
        {
            var value = new List<byte[]>
        {
            new byte[] { 0x01 },
            new byte[] { 0x02 }
        };
            var result = InvokeV2(value);
            Assert.Equal(2, result.L.Count);
            Assert.NotNull(result.L[0].B);
        }

        [Fact]
        public void ConvertToAttributeValueV2_Array_Numeric_StoredAsL()
        {
            var value = new double[] { 1.1, 2.2 };
            var result = InvokeV2(value);
            Assert.Equal(2, result.L.Count);
            Assert.Equal("1.1", result.L[0].N);
            Assert.Equal("2.2", result.L[1].N);
        }

        [Fact]
        public void ConvertToAttributeValueV2_ByteStoredAsN()
        {
            var value = (byte)99;
            var result = InvokeV2(value);
            Assert.Equal("99", result.N);
        }

        // --- Helpers ---
        private AttributeValue InvokeV1(object value) => typeof(DynamoDbMapper)
            .GetMethod("ConvertToAttributeValueV1", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?.Invoke(null, new[] { value }) as AttributeValue;

        private AttributeValue InvokeV2(object value) => typeof(DynamoDbMapper)
            .GetMethod("ConvertToAttributeValueV2", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
            ?.Invoke(null, new[] { value }) as AttributeValue;
    }

}
