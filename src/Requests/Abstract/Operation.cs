using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions.Requests.Abstract
{
    public class Operation
    {
        private Operation(Delete operation)
        {
            Type = TransactOperationType.Delete;
            DeleteType = operation;
        }

        private Operation(Update operation)
        {
            Type = TransactOperationType.Update;
            UpdateType = operation;
        }

        private Operation(Put operation)
        {
            Type = TransactOperationType.Put;
            PutType = operation;
        }

        private Operation(ConditionCheck operation)
        {
            Type = TransactOperationType.ConditionCheck;
            ConditionCheckType = operation;
        }

        private Operation(Update operation, TransactOperationType type = TransactOperationType.Patch)
        {
            Type = TransactOperationType.Patch;
            UpdateType = operation;
        }

        public TransactOperationType Type { get; private set; }

        public Delete? DeleteType { get; private set; }
        public Put? PutType { get; private set; }
        public Update? UpdateType { get; private set; }
        public ConditionCheck? ConditionCheckType { get; private set; }

        public static Operation Delete(Delete operation)
        {
            var obj = new Operation(operation);
            return obj;
        }

        public static Operation Put(Put operation)
        {
            var obj = new Operation(operation);
            return obj;
        }

        public static Operation Update(Update operation)
        {
            var obj = new Operation(operation);
            return obj;
        }

        public static Operation Patch(Update operation)
        {
            var obj = new Operation(operation, TransactOperationType.Patch);
            return obj;
        }

        public static Operation Check(ConditionCheck operation)
        {
            var obj = new Operation(operation);
            return obj;
        }
    }
}
