using System.Globalization;
using Amazon.DynamoDBv2;
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
            if (propertyName is not null && convertedItem.TryGetValue(propertyName, out var versionAttr))
            {
                string nextValue = "0";

                if (versionAttr.NULL == true)
                {
                    convertedItem[propertyName] = new AttributeValue { N = "0" };
                }
                else
                {
                    var currentValue = long.Parse(versionAttr.N, CultureInfo.InvariantCulture);
                    nextValue = (currentValue + 1).ToString(CultureInfo.InvariantCulture);

                    ExpressionAttributeNames.Add("#Version", propertyName);
                    ExpressionAttributeValues.Add(":expectedVersion", new AttributeValue { N = currentValue.ToString(CultureInfo.InvariantCulture) });
                    ConditionExpression = "#Version = :expectedVersion";
                }

                convertedItem[propertyName].N = nextValue;
            }
        }

        public Type ItemType { get; init; }
        public string TableName { get; private set; }
        public Dictionary<string, AttributeValue> Key { get; set; } = new Dictionary<string, AttributeValue>(2);
        public string? ConditionExpression { get; set; }
        public abstract TransactOperationType Type { get; }
        public Dictionary<string, string> ExpressionAttributeNames { get; set; } = new Dictionary<string, string>(4);
        public Dictionary<string, AttributeValue> ExpressionAttributeValues { get; set; } = new Dictionary<string, AttributeValue>(4);

        public ReturnValuesOnConditionCheckFailure? ReturnValuesOnConditionCheckFailure { get; set; }

        public abstract Operation GetOperation();

        /// <summary>
        /// Converts a key value (string, numeric, or binary) to an <see cref="AttributeValue"/>.
        /// </summary>
        /// <param name="keyValue">The key value to convert (string, int, long, decimal, double, float, or byte[]).</param>
        /// <returns>The corresponding <see cref="AttributeValue"/>.</returns>
        protected static AttributeValue ToKeyAttributeValue(object keyValue)
        {
            ArgumentNullException.ThrowIfNull(keyValue);

            if (keyValue is string s)
            {
                return new AttributeValue { S = s };
            }

            var attr = DynamoDbMapper.GetAttributeValue(keyValue);
            if (attr == null)
            {
                throw new ArgumentException(
                    $"Cannot convert key value of type '{keyValue.GetType().Name}' to a DynamoDB AttributeValue.",
                    nameof(keyValue));
            }

            return attr;
        }

        protected Dictionary<string, AttributeValue> GetKey(KeyValue keyValue)
        {
            var key = new Dictionary<string, AttributeValue>
            {
                { DynamoDbMapper.GetPropertyAttributedName(ItemType, keyValue.Key), new AttributeValue { S = keyValue.Value } }
            };

            return key;
        }

        private static string SetTableName(Type item)
        {
            return DynamoDbMapper.GetTableName(item);
        }
    }
}
