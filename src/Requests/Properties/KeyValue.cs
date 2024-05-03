namespace DynamoDBv2.Transactions.Requests.Properties
{
    /// <summary>
    /// Helping class to define key name and value of the HASH key on the table.
    /// </summary>
    public struct KeyValue
    {
        public string Key { get; set; }

        public string Value { get; set; }
    }
}
