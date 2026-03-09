using System.Linq.Expressions;
using System.Text;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;
using DynamoDBv2.Transactions.Requests.Properties;

namespace DynamoDBv2.Transactions.Requests
{
    /// <summary>
    /// Represents a request to perform a check that an item exists or to check the condition
    /// of specific attributes of the item.
    /// </summary>
    public sealed class ConditionCheckTransactionRequest<T> : TransactionRequest
    {
        private readonly StringBuilder _conditionBuilder = new();

        public override TransactOperationType Type => TransactOperationType.ConditionCheck;

        /// <summary>
        /// Checks condition on item by its HASH key value, assumes that <see cref="DynamoDBHashKeyAttribute"/> is set.
        /// </summary>
        /// <param name="keyValue">Value of the Key</param>
        public ConditionCheckTransactionRequest(string keyValue)
            : base(typeof(T))
        {
            var keyNameAttributed = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
            var keyAttribute = new AttributeValue { S = keyValue };
            var key = new Dictionary<string, AttributeValue> { { keyNameAttributed, keyAttribute } };
            Key = key;
        }

        public override Operation GetOperation()
        {
            if (_conditionBuilder.Length >= 5)
            {
                _conditionBuilder.Length -= 5; // trim trailing " AND "
                ConditionExpression = _conditionBuilder.ToString();
            }

            var check = new ConditionCheck
            {
                TableName = TableName,
                Key = Key
            };

            if (ExpressionAttributeNames.Count > 0)
            {
                check.ExpressionAttributeNames = ExpressionAttributeNames;
            }

            if (ExpressionAttributeValues.Count > 0)
            {
                check.ExpressionAttributeValues = ExpressionAttributeValues;
            }

            if (!String.IsNullOrEmpty(ConditionExpression))
            {
                check.ConditionExpression = ConditionExpression;
            }

            return Operation.Check(check);
        }

        public void Equals<TV, TValue>(Expression<Func<TV, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, "=", value);
        }

        public void NotEquals<TV, TValue>(Expression<Func<TV, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, "<>", value);
        }

        public void GreaterThan<TV, TValue>(Expression<Func<TV, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, ">", value);
        }

        public void LessThan<TV, TValue>(Expression<Func<TV, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, "<", value);
        }

        public void VersionEquals<TV>(Expression<Func<TV, long?>> propertyExpression, long? expectedVersion)
        {
            // This assumes that version is a number and stored in DynamoDB as a numeric type
            AddCondition<TV, long?>(propertyExpression, "=", expectedVersion);
        }

        private void AddCondition<TV, TValue>(Expression<Func<TV, TValue>> propertyExpression, string comparisonOperator, TValue value)
        {
            var propertyName = GetPropertyName(propertyExpression);
            var attributeValue = DynamoDbMapper.GetAttributeValue(value!);

            ExpressionAttributeNames[$"#{propertyName}"] = propertyName;
            ExpressionAttributeValues[$":{propertyName}Value"] = attributeValue!;
            _conditionBuilder.Append($"#{propertyName} {comparisonOperator} :{propertyName}Value AND ");
        }

        private string GetPropertyName<TV, TValue>(Expression<Func<TV, TValue>> expression)
        {
            string? propertyName = null;

            if (expression.Body is MemberExpression member)
            {
                propertyName = member.Member.Name;
            }

            if (expression.Body is UnaryExpression unaryMember)
            {
                propertyName = ((MemberExpression)unaryMember.Operand).Member.Name;
            }

            if (string.IsNullOrEmpty(propertyName))
            {
                throw new ArgumentNullException("Property Name not found.");
            }

            propertyName = DynamoDbMapper.GetPropertyAttributedName(ItemType, propertyName!);

            return propertyName;
        }
    }
}
