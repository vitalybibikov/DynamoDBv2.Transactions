using System.Linq.Expressions;
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
        public override TransactOperationType Type => TransactOperationType.ConditionCheck;

        public ConditionCheckTransactionRequest(KeyValue keyValue)
            : base(typeof(T))
        {
            Initialize(keyValue);
        }

        public ConditionCheckTransactionRequest(string keyValue)
            : base(typeof(T))
        {
            var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));
            Initialize(new KeyValue { Key = key, Value = keyValue });
        }

        public ConditionCheckTransactionRequest(T model)
            : base(typeof(T))
        {
            if (model == null)
            {
                throw new ArgumentNullException(nameof(model));
            }

            var attributes = DynamoDbMapper.MapToAttribute(model);
            var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));

            var keyValue = attributes[key];

            Initialize(new KeyValue { Key = key, Value = keyValue.S });
        }

        public override Operation GetOperation()
        {
            // Ensure the final condition expression does not end with "AND "
            if (!String.IsNullOrEmpty(ConditionExpression))
            {
                ConditionExpression = ConditionExpression.TrimEnd(' ', 'A', 'N', 'D');
            }
            
            var check = new ConditionCheck
            {
                TableName = TableName,
                Key = Key,
                ExpressionAttributeNames = ExpressionAttributeNames,
                ExpressionAttributeValues = ExpressionAttributeValues,
                ConditionExpression = ConditionExpression
            };

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
            ConditionExpression += $"{ConditionExpression} #{propertyName} {comparisonOperator} :{propertyName}Value AND ";
        }

        private void Initialize(KeyValue keyValue)
        {
            Key = GetKey(keyValue);
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
