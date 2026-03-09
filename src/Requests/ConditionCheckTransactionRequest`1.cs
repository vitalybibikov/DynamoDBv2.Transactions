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
        private int _conditionCounter = 0;

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

        /// <summary>
        /// Checks condition on item by its HASH + RANGE key values.
        /// </summary>
        /// <param name="hashKeyValue">Value of the hash key.</param>
        /// <param name="rangeKeyValue">Value of the range key.</param>
        public ConditionCheckTransactionRequest(string hashKeyValue, string rangeKeyValue)
            : base(typeof(T))
        {
            var hashKeyName = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
            var rangeKeyName = DynamoDbMapper.GetRangeKeyAttributeName(typeof(T));
            Key = new Dictionary<string, AttributeValue>
            {
                { hashKeyName, new AttributeValue { S = hashKeyValue } },
                { rangeKeyName, new AttributeValue { S = rangeKeyValue } }
            };
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

            if (ReturnValuesOnConditionCheckFailure != null)
            {
                check.ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure;
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

            var nameToken = $"#p{_conditionCounter}";
            var valueToken = $":v{_conditionCounter}";
            _conditionCounter++;

            ExpressionAttributeNames[nameToken] = propertyName;
            ExpressionAttributeValues[valueToken] = attributeValue!;
            _conditionBuilder.Append($"{nameToken} {comparisonOperator} {valueToken} AND ");
        }

        private string GetPropertyName<TV, TValue>(Expression<Func<TV, TValue>> expression)
        {
            string? propertyName = null;

            if (expression.Body is MemberExpression member)
            {
                propertyName = member.Member.Name;
            }
            else if (expression.Body is UnaryExpression unaryMember && unaryMember.Operand is MemberExpression innerMember)
            {
                propertyName = innerMember.Member.Name;
            }

            if (string.IsNullOrEmpty(propertyName))
            {
                throw new ArgumentException("Could not extract property name from expression.");
            }

            propertyName = DynamoDbMapper.GetPropertyAttributedName(ItemType, propertyName!);

            return propertyName;
        }
    }
}
