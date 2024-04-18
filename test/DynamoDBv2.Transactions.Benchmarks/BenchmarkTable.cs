using Amazon.DynamoDBv2.DataModel;
using DynamoDBv2.Transactions.IntegrationTests.Models;

namespace DynamoDBv2.Transactions.Benchmarks
{
    public class BenchmarkTable : ITransactional
    {
        [DynamoDBHashKey("UserId")]
        public string UserId { get; set; }

        [DynamoDBProperty("SomeInt1")]
        public int SomeInt { get; set; }

        [DynamoDBProperty("SomeNullableInt32")]
        public int? SomeNullableInt32 { get; set; }

        [DynamoDBProperty("SomeLong1")]
        public int SomeLong { get; set; }

        [DynamoDBProperty("SomeNullableLong")]
        public int? SomeNullableLong { get; set; }

        [DynamoDBProperty("SomeFloat1")]
        public float SomeFloat { get; set; }

        [DynamoDBProperty("SomeNullableFloat")]
        public float? SomeNullableFloat { get; set; }

        [DynamoDBProperty("SomeDecimal1")]
        public decimal SomeDecimal { get; set; }

        [DynamoDBProperty("SomeNullableDecimal")]
        public decimal? SomeNullableDecimal { get; set; }

        [DynamoDBProperty("SomeDate1")]
        public DateTime SomeDate { get; set; }

        [DynamoDBProperty("SomeNullableDate1")]
        public DateTime? SomeNullableDate1 { get; set; }

        [DynamoDBProperty("SomeBool1")]
        public bool SomeBool { get; set; }

        [DynamoDBProperty("SomeNullableBool")]
        public bool? SomeNullableBool { get; set; }

        [DynamoDBProperty("SomeClass")]
        public SomeClass? SomeClass { get; set; }

        [DynamoDBProperty("SomeRecord")]
        public SomeRecord? SomeRecord { get; set; }
        
        [DynamoDBProperty("SomeClassList")]
        public List<SomeClass>? SomeClassList { get; set; }

        [DynamoDBProperty("SomeClassDictionary")]
        public Dictionary<string,SomeClass>? SomeClassDictionary { get; set; }

        [DynamoDBProperty("SomeMemoryStream")]
        public MemoryStream SomeMemoryStream { get; set; }

        [DynamoDBProperty("SomeBytes")]
        public byte[] SomeBytes { get; set; }

        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
