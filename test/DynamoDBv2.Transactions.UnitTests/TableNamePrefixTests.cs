using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests for fix #15: TableNamePrefix is prepended to all resolved table names.
    /// IMPORTANT: Each test resets TableNamePrefix to null in a finally block to
    /// prevent cross-test pollution.
    /// </summary>
    [Collection("TableNamePrefix")]
    public class TableNamePrefixTests
    {
        [Fact]
        public void GetTableName_WithPrefix_PrependsPrefix()
        {
            try
            {
                DynamoDbMapper.TableNamePrefix = "staging-";

                // SomeDynamoDbEntity has [DynamoDBTable("SomeDynamoDbEntity")]
                var result = DynamoDbMapper.GetTableName(typeof(SomeDynamoDbEntity));

                Assert.Equal("staging-SomeDynamoDbEntity", result);
            }
            finally
            {
                DynamoDbMapper.TableNamePrefix = null;
            }
        }

        [Fact]
        public void GetTableName_WithNullPrefix_ReturnsOriginalName()
        {
            try
            {
                DynamoDbMapper.TableNamePrefix = null;

                var result = DynamoDbMapper.GetTableName(typeof(SomeDynamoDbEntity));

                Assert.Equal("SomeDynamoDbEntity", result);
            }
            finally
            {
                DynamoDbMapper.TableNamePrefix = null;
            }
        }

        [Fact]
        public void GetTableName_WithEmptyPrefix_ReturnsOriginalName()
        {
            try
            {
                DynamoDbMapper.TableNamePrefix = "";

                var result = DynamoDbMapper.GetTableName(typeof(SomeDynamoDbEntity));

                Assert.Equal("SomeDynamoDbEntity", result);
            }
            finally
            {
                DynamoDbMapper.TableNamePrefix = null;
            }
        }

        [Fact]
        public void GetTableName_SourceGenPath_WithPrefix()
        {
            try
            {
                DynamoDbMapper.TableNamePrefix = "dev-";

                // SomeDynamoDbEntity is partial → registered via source gen
                var result = DynamoDbMapper.GetTableName(typeof(SomeDynamoDbEntity));

                Assert.Equal("dev-SomeDynamoDbEntity", result);
            }
            finally
            {
                DynamoDbMapper.TableNamePrefix = null;
            }
        }

        [Fact]
        public void GetTableName_ReflectionPath_WithPrefix()
        {
            try
            {
                DynamoDbMapper.TableNamePrefix = "qa-";

                // IgnoreReflectionTestEntity is NOT partial → reflection fallback
                // [DynamoDBTable("IgnoreReflTable")]
                var result = DynamoDbMapper.GetTableName(typeof(IgnoreReflectionTestEntity));

                Assert.Equal("qa-IgnoreReflTable", result);
            }
            finally
            {
                DynamoDbMapper.TableNamePrefix = null;
            }
        }

        [Fact]
        public void GetTableName_TypeNameFallback_WithPrefix()
        {
            try
            {
                DynamoDbMapper.TableNamePrefix = "prod-";

                // SomeNotAttributedDynamoDbEntity has no [DynamoDBTable] → falls back to type name
                var result = DynamoDbMapper.GetTableName(typeof(SomeNotAttributedDynamoDbEntity));

                Assert.Equal("prod-SomeNotAttributedDynamoDbEntity", result);
            }
            finally
            {
                DynamoDbMapper.TableNamePrefix = null;
            }
        }

        [Fact]
        public void TransactionRequest_UsesPrefix_InTableName()
        {
            try
            {
                DynamoDbMapper.TableNamePrefix = "test-";

                // Create a DeleteTransactionRequest — its TableName is set in the constructor
                var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("key-123");

                Assert.Equal("test-SomeDynamoDbEntity", request.TableName);
            }
            finally
            {
                DynamoDbMapper.TableNamePrefix = null;
            }
        }
    }
}
