﻿using System.Linq.Expressions;
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
        public override TransactOperationType Type => TransactOperationType.ConditionCheck;

        public ConditionCheckTransactionRequest(KeyValue keyValue)
            : base(typeof(T))
        {
            Key = GetKey(keyValue);
        }

        public ConditionCheckTransactionRequest(string keyValue)
            : base(typeof(T))
        {
            var key = DynamoDbMapper.GetHashKeyAttributeName(typeof(T));

            Key = GetKey(new KeyValue
            {
                Key = key,
                Value = keyValue
            });
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

            Key = GetKey(new KeyValue
            {
                Key = key,
                Value = keyValue.S
            });
        }

        public override Operation GetOperation()
        {
            // Ensure the final condition expression does not end with "AND "
            ConditionExpression = ConditionExpression.TrimEnd(' ', 'A', 'N', 'D');

            var check = new ConditionCheck
            {
                TableName = TableName,
                Key = Key,
                ConditionExpression = ConditionExpression
            };

            return Operation.Check(check);
        }

        public void Equals<T, TValue>(Expression<Func<T, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, "=", value);
        }

        public void NotEquals<T, TValue>(Expression<Func<T, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, "<>", value);
        }

        public void GreaterThan<T, TValue>(Expression<Func<T, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, ">", value);
        }

        public void LessThan<T, TValue>(Expression<Func<T, TValue>> propertyExpression, TValue value)
        {
            AddCondition(propertyExpression, "<", value);
        }

        public void VersionEquals<T>(Expression<Func<T, long>> propertyExpression, long expectedVersion)
        {
            // This assumes that version is a number and stored in DynamoDB as a numeric type
            AddCondition<T, long>(propertyExpression, "=", expectedVersion);
        }

        private void AddCondition<T, TValue>(Expression<Func<T, TValue>> propertyExpression, string comparisonOperator, TValue value)
        {
            var propertyName = GetPropertyName(propertyExpression);
            var attributeValue = DynamoDbMapper.GetAttributeValue(value);

            ExpressionAttributeNames[$"#{propertyName}"] = propertyName;
            ExpressionAttributeValues[$":{propertyName}Value"] = attributeValue;
            ConditionExpression += $"{ConditionExpression} #{propertyName} {comparisonOperator} :{propertyName}Value AND ";
        }

        private string GetPropertyName<T, TValue>(Expression<Func<T, TValue>> expression)
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
