using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    /// <summary>
    /// Tests for all request type constructors and their GetOperation methods,
    /// covering all overloads and edge cases.
    /// </summary>
    public class RequestConstructorTests
    {
        #region DeleteTransactionRequest

        [Fact]
        public void Delete_DictionaryConstructor_SetsKey()
        {
            var key = new Dictionary<string, AttributeValue>
            {
                { "pk", new AttributeValue { S = "val1" } },
                { "sk", new AttributeValue { S = "val2" } }
            };

            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>(key);

            Assert.Equal(2, request.Key.Count);
            Assert.Equal("val1", request.Key["pk"].S);
            Assert.Equal("val2", request.Key["sk"].S);
        }

        [Fact]
        public void Delete_KeyValueConstructor_MapsToAttributedName()
        {
            var kv = new KeyValue { Key = "Id", Value = "abc" };

            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>(kv);

            // SomeDynamoDbEntity has [DynamoDBHashKey(AttributeName = "MyId")] on Id
            Assert.Contains("MyId", request.Key.Keys);
            Assert.Equal("abc", request.Key["MyId"].S);
        }

        [Fact]
        public void Delete_StringConstructor_UsesHashKeyAttribute()
        {
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("deleteMe");

            Assert.Contains("MyId", request.Key.Keys);
            Assert.Equal("deleteMe", request.Key["MyId"].S);
        }

        [Fact]
        public void Delete_StringConstructor_NonAttributed_ThrowsWithoutHashKey()
        {
            // SomeNotAttributedDynamoDbEntity has no [DynamoDBHashKey] attribute
            Assert.Throws<ArgumentException>(() =>
                new DeleteTransactionRequest<SomeNotAttributedDynamoDbEntity>("deleteMe"));
        }

        [Fact]
        public void Delete_Type_IsDelete()
        {
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("val");

            Assert.Equal(TransactOperationType.Delete, request.Type);
        }

        [Fact]
        public void Delete_GetOperation_SetsTableName()
        {
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("val");
            var op = request.GetOperation();

            Assert.Equal("SomeDynamoDbEntity", op.DeleteType!.TableName);
        }

        #endregion

        #region PutTransactionRequest

        [Fact]
        public void Put_MapsAllProperties()
        {
            var entity = new SomeDynamoDbEntity
            {
                Id = "1",
                Name = "Test",
                Status = "Active",
                Amount = 99.5,
                Version = null
            };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            Assert.Contains("MyId", request.PutRequest.Item.Keys);
            Assert.Contains("Name", request.PutRequest.Item.Keys);
            Assert.Contains("Status", request.PutRequest.Item.Keys);
            Assert.Contains("Amount", request.PutRequest.Item.Keys);
        }

        [Fact]
        public void Put_TableName_FromDynamoDBTableAttribute()
        {
            var entity = new SomeDynamoDbEntity { Id = "1" };

            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            Assert.Equal("SomeDynamoDbEntity", request.TableName);
        }

        [Fact]
        public void Put_NullItem_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new PutTransactionRequest<SomeDynamoDbEntity>(null!));
        }

        [Fact]
        public void Put_Type_IsPut()
        {
            var entity = new SomeDynamoDbEntity { Id = "1" };
            var request = new PutTransactionRequest<SomeDynamoDbEntity>(entity);

            Assert.Equal(TransactOperationType.Put, request.Type);
        }

        #endregion

        #region PatchTransactionRequest

        [Fact]
        public void Patch_KeyValueConstructor_SetsUpdateExpression()
        {
            var kv = new KeyValue { Key = "Id", Value = "1" };
            var prop = new Property { Name = "Status", Value = "Updated" };

            var request = new PatchTransactionRequest<SomeDynamoDbEntity>(kv, prop);

            Assert.Equal("SET #Property = :newValue", request.UpdateExpression);
            Assert.Equal("Status", request.ExpressionAttributeNames["#Property"]);
        }

        [Fact]
        public void Patch_StringKeyConstructor_SetsKeyFromHashAttribute()
        {
            var prop = new Property { Name = "Status", Value = "Updated" };

            var request = new PatchTransactionRequest<SomeDynamoDbEntity>("keyVal", prop);

            Assert.Contains("MyId", request.Key.Keys);
            Assert.Equal("keyVal", request.Key["MyId"].S);
        }

        [Fact]
        public void Patch_ModelConstructor_ExtractsKeyAndValue()
        {
            var entity = new SomeDynamoDbEntity { Id = "entity1", Name = "Test", Status = "Active" };

            var request = new PatchTransactionRequest<SomeDynamoDbEntity>(entity, "Status");

            Assert.Contains("MyId", request.Key.Keys);
            Assert.Equal("SET #Property = :newValue", request.UpdateExpression);
        }

        [Fact]
        public void Patch_ModelConstructor_NullModel_Throws()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new PatchTransactionRequest<SomeDynamoDbEntity>(null!, "Status"));
        }

        [Fact]
        public void Patch_ModelConstructor_NullPropertyName_Throws()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new PatchTransactionRequest<SomeDynamoDbEntity>(new SomeDynamoDbEntity { Id = "1" }, null!));
        }

        [Fact]
        public void Patch_Type_IsPatch()
        {
            var kv = new KeyValue { Key = "Id", Value = "1" };
            var prop = new Property { Name = "Status", Value = "test" };
            var request = new PatchTransactionRequest<SomeDynamoDbEntity>(kv, prop);

            Assert.Equal(TransactOperationType.Patch, request.Type);
        }

        [Fact]
        public void Patch_GetOperation_ReturnsPatchOperation()
        {
            var kv = new KeyValue { Key = "Id", Value = "1" };
            var prop = new Property { Name = "Status", Value = "Active" };
            var request = new PatchTransactionRequest<SomeDynamoDbEntity>(kv, prop);

            var op = request.GetOperation();

            Assert.Equal(TransactOperationType.Patch, op.Type);
            Assert.NotNull(op.UpdateType);
            Assert.Equal("SomeDynamoDbEntity", op.UpdateType!.TableName);
            Assert.Equal("SET #Property = :newValue", op.UpdateType.UpdateExpression);
        }

        #endregion

        #region UpdateTransactionRequest

        [Fact]
        public void Update_DefaultConstructor_NullExpression()
        {
            var request = new UpdateTransactionRequest<SomeDynamoDbEntity>();

            Assert.Null(request.UpdateExpression);
            Assert.Equal(TransactOperationType.Update, request.Type);
        }

        [Fact]
        public void Update_WithExpression_SetsExpression()
        {
            var request = new UpdateTransactionRequest<SomeDynamoDbEntity>("SET #name = :val");

            Assert.Equal("SET #name = :val", request.UpdateExpression);
        }

        [Fact]
        public void Update_GetOperation_WithExpressionAttributes()
        {
            var request = new UpdateTransactionRequest<SomeDynamoDbEntity>("SET #name = :val");
            request.ExpressionAttributeNames["#name"] = "Name";
            request.ExpressionAttributeValues[":val"] = new AttributeValue { S = "NewName" };
            request.Key = new Dictionary<string, AttributeValue>
            {
                { "MyId", new AttributeValue { S = "1" } }
            };

            var op = request.GetOperation();
            var update = op.UpdateType!;

            Assert.Equal("SET #name = :val", update.UpdateExpression);
            Assert.Equal("Name", update.ExpressionAttributeNames["#name"]);
            Assert.Equal("NewName", update.ExpressionAttributeValues[":val"].S);
        }

        [Fact]
        public void Update_GetOperation_WithConditionExpression()
        {
            var request = new UpdateTransactionRequest<SomeDynamoDbEntity>("SET #a = :v");
            request.ConditionExpression = "attribute_exists(MyId)";

            var op = request.GetOperation();

            Assert.Equal("attribute_exists(MyId)", op.UpdateType!.ConditionExpression);
        }

        #endregion

        #region Table name resolution

        [Fact]
        public void TableName_WithDynamoDBTableAttribute_UsesAttributeValue()
        {
            var request = new DeleteTransactionRequest<SomeDynamoDbEntity>("val");

            Assert.Equal("SomeDynamoDbEntity", request.TableName);
        }

        [Fact]
        public void TableName_WithoutDynamoDBTableAttribute_UsesTypeName()
        {
            var request = new DeleteTransactionRequest<SimpleHashKeyEntity>("val");

            Assert.Equal("SimpleHashKeyEntity", request.TableName);
        }

        #endregion
    }
}
