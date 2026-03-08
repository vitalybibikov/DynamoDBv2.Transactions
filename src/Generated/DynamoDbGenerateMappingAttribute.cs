namespace DynamoDBv2.Transactions;

/// <summary>
/// Gets or sets a value indicating whether to generate compile-time DynamoDB mapping for this type.
/// Place on a partial class to enable zero-reflection attribute mapping.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class DynamoDbGenerateMappingAttribute : Attribute
{
}
