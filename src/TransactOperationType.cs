namespace DynamoDBv2.Transactions
{
    /// <summary>
    /// Custom Transact Operation Type for DynamoDB
    /// </summary>
    public enum TransactOperationType
    {
        Put,
        Update,
        Delete,
        ConditionCheck,
        Patch
    }
}
