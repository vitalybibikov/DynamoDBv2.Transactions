using Amazon.DynamoDBv2.DataModel;

namespace DynamoDBv2.Transactions.Benchmarks.Entities
{
    /// <summary>
    /// Shipping address nested within an order.
    /// </summary>
    public class Address
    {
        [DynamoDBProperty("Street")]
        public string Street { get; set; }

        [DynamoDBProperty("City")]
        public string City { get; set; }

        [DynamoDBProperty("Country")]
        public string Country { get; set; }

        [DynamoDBProperty("PostalCode")]
        public string PostalCode { get; set; }
    }

    /// <summary>
    /// A single line item within an order.
    /// </summary>
    public class OrderItem
    {
        [DynamoDBProperty("SKU")]
        public string SKU { get; set; }

        [DynamoDBProperty("ProductName")]
        public string ProductName { get; set; }

        [DynamoDBProperty("Quantity")]
        public int Quantity { get; set; }

        [DynamoDBProperty("UnitPrice")]
        public decimal UnitPrice { get; set; }

        [DynamoDBProperty("Discount")]
        public decimal? Discount { get; set; }
    }

    /// <summary>
    /// Realistic e-commerce order entity (~20 properties).
    /// Partial class — source-generator path in DynamoDbMapper.
    /// </summary>
    [DynamoDBTable("Orders")]
    public partial class OrderEntity : ITransactional
    {
        [DynamoDBHashKey("OrderId")]
        public string OrderId { get; set; }

        // --- Strings ---

        [DynamoDBProperty("CustomerName")]
        public string CustomerName { get; set; }

        [DynamoDBProperty("Email")]
        public string Email { get; set; }

        [DynamoDBProperty("Status")]
        public string Status { get; set; }

        [DynamoDBProperty("ShippingMethod")]
        public string ShippingMethod { get; set; }

        [DynamoDBProperty("TrackingNumber")]
        public string TrackingNumber { get; set; }

        // --- Nested object ---

        [DynamoDBProperty("ShippingAddress")]
        public Address ShippingAddress { get; set; }

        // --- Collection types ---

        [DynamoDBProperty("OrderItems")]
        public List<OrderItem> OrderItems { get; set; }

        [DynamoDBProperty("Metadata")]
        public Dictionary<string, string> Metadata { get; set; }

        // --- Numerics ---

        [DynamoDBProperty("SubTotal")]
        public decimal SubTotal { get; set; }

        [DynamoDBProperty("TaxAmount")]
        public decimal TaxAmount { get; set; }

        [DynamoDBProperty("ShippingCost")]
        public decimal ShippingCost { get; set; }

        [DynamoDBProperty("TotalAmount")]
        public decimal TotalAmount { get; set; }

        // --- Dates ---

        [DynamoDBProperty("OrderedAt")]
        public DateTime OrderedAt { get; set; }

        [DynamoDBProperty("ShippedAt")]
        public DateTime? ShippedAt { get; set; }

        [DynamoDBProperty("DeliveredAt")]
        public DateTime? DeliveredAt { get; set; }

        // --- Bools ---

        [DynamoDBProperty("IsPriority")]
        public bool IsPriority { get; set; }

        [DynamoDBProperty("RequiresSignature")]
        public bool RequiresSignature { get; set; }

        // --- Version ---

        [DynamoDBVersion]
        public long? Version { get; set; }
    }

    /// <summary>
    /// Non-partial entity — forces reflection path in DynamoDbMapper.
    /// Identical properties to OrderEntity for fair comparison.
    /// </summary>
    [DynamoDBTable("Orders")]
    public class ReflectionOrderEntity : ITransactional
    {
        [DynamoDBHashKey("OrderId")]
        public string OrderId { get; set; }

        // --- Strings ---

        [DynamoDBProperty("CustomerName")]
        public string CustomerName { get; set; }

        [DynamoDBProperty("Email")]
        public string Email { get; set; }

        [DynamoDBProperty("Status")]
        public string Status { get; set; }

        [DynamoDBProperty("ShippingMethod")]
        public string ShippingMethod { get; set; }

        [DynamoDBProperty("TrackingNumber")]
        public string TrackingNumber { get; set; }

        // --- Nested object ---

        [DynamoDBProperty("ShippingAddress")]
        public Address ShippingAddress { get; set; }

        // --- Collection types ---

        [DynamoDBProperty("OrderItems")]
        public List<OrderItem> OrderItems { get; set; }

        [DynamoDBProperty("Metadata")]
        public Dictionary<string, string> Metadata { get; set; }

        // --- Numerics ---

        [DynamoDBProperty("SubTotal")]
        public decimal SubTotal { get; set; }

        [DynamoDBProperty("TaxAmount")]
        public decimal TaxAmount { get; set; }

        [DynamoDBProperty("ShippingCost")]
        public decimal ShippingCost { get; set; }

        [DynamoDBProperty("TotalAmount")]
        public decimal TotalAmount { get; set; }

        // --- Dates ---

        [DynamoDBProperty("OrderedAt")]
        public DateTime OrderedAt { get; set; }

        [DynamoDBProperty("ShippedAt")]
        public DateTime? ShippedAt { get; set; }

        [DynamoDBProperty("DeliveredAt")]
        public DateTime? DeliveredAt { get; set; }

        // --- Bools ---

        [DynamoDBProperty("IsPriority")]
        public bool IsPriority { get; set; }

        [DynamoDBProperty("RequiresSignature")]
        public bool RequiresSignature { get; set; }

        // --- Version ---

        [DynamoDBVersion]
        public long? Version { get; set; }
    }
}
