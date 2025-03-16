using System.Reflection;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions.Requests.Abstract
{
    public abstract class TransactionRequest : ITransactionRequest
    {
        protected TransactionRequest(Type item)
        {
            ItemType = item;
            TableName = SetTableName(item);
        }

        protected virtual void SetVersion<T>(Dictionary<string, AttributeValue> convertedItem, string? propertyName)
        {
            if (propertyName is not null)
            {
                string nextValue = 0.ToString();

                if (convertedItem[propertyName].NULL.HasValue)
                {
                    convertedItem[propertyName] = new AttributeValue { N = 0.ToString() };
                }
                else
                {
                    var currentValue = long.Parse(convertedItem[propertyName].N);
                    nextValue = (currentValue + 1).ToString();

                    ExpressionAttributeNames.Add("#Version", propertyName);
                    ExpressionAttributeValues.Add(":expectedVersion", new AttributeValue { N = currentValue.ToString() });
                    ConditionExpression = "#Version = :expectedVersion";
                }

                convertedItem[propertyName].N = nextValue;
            }
        }

        public Type ItemType { get; init; }
        public string TableName { get; private set; }
        public Dictionary<string, AttributeValue> Key { get; set; } = new Dictionary<string, AttributeValue>();
        public string? ConditionExpression { get; set; }
        public abstract TransactOperationType Type { get; }
        public Dictionary<string, string> ExpressionAttributeNames { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, AttributeValue> ExpressionAttributeValues { get; set; } = new Dictionary<string, AttributeValue>();

        public abstract Operation GetOperation();

        protected Dictionary<string, AttributeValue> GetKey(KeyValue keyValue)
        {
            var key = new Dictionary<string, AttributeValue>
            {
                { DynamoDbMapper.GetPropertyAttributedName(ItemType, keyValue.Key), new AttributeValue { S = keyValue.Value } }
            };

            return key;
        }

        private string SetTableName(Type item)
        {
            string? dynamoDbTable = null;
            var tableAttribute = item.GetCustomAttribute<DynamoDBTableAttribute>();

            if (tableAttribute != null)
            {
                dynamoDbTable = tableAttribute.TableName;
            }

            if (string.IsNullOrEmpty(dynamoDbTable))
            {
                dynamoDbTable = item.Name;
            }

            return dynamoDbTable;
        }
    }
}
