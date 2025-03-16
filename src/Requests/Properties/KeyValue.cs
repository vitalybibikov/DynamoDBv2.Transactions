namespace DynamoDBv2.Transactions.Requests.Properties
{
    /// <summary>
    /// Helping class to define key name and value of the HASH key on the table.
    /// </summary>
    public struct KeyValue
    {
        public required string Key { get; set; }

        public required string Value { get; set; }
    }
}
