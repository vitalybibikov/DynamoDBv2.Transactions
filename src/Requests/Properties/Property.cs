namespace DynamoDBv2.Transactions.Requests.Properties
{
    /// <summary>
    /// Helping class to define key name and value of the Property on the table.
    /// </summary>
    public class Property
    {
        public string Name { get; set; }

        public object? Value { get; set; }
    }
}
