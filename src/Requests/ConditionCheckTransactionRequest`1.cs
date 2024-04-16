using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests.Abstract;

namespace DynamoDBv2.Transactions.Requests
{
    public class ConditionCheckTransactionRequest<T> : TransactionRequest
    {
        public override TransactOperationType Type => TransactOperationType.ConditionCheck;

        public ConditionCheckTransactionRequest()
            : base(typeof(T))
        {
        }

        public override Operation GetOperation()
        {
            var check = new ConditionCheck
            {
                TableName = TableName,
                Key = Key,
                ConditionExpression = ConditionExpression,
                ExpressionAttributeNames = ExpressionAttributeNames,
                ExpressionAttributeValues = ExpressionAttributeValues
            };

            return Operation.Check(check);
        }
    }
}
