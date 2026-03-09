using System.Linq.Expressions;
using System.Text;

namespace DynamoDBv2.Transactions.Requests;

/// <summary>
/// Builds DynamoDB projection expressions from LINQ expressions.
/// </summary>
public static class ProjectionBuilder
{
    /// <summary>
    /// Builds a projection expression and expression attribute names from a lambda expression.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="projection">A lambda like <c>x => new { x.Name, x.Status }</c>.</param>
    /// <returns>A tuple of projection expression string and attribute name mappings.</returns>
    public static (string ProjectionExpression, Dictionary<string, string> ExpressionAttributeNames) Build<T>(
        Expression<Func<T, object>> projection)
    {
        var propertyNames = ExtractPropertyNames(projection);

        if (propertyNames.Count == 0)
        {
            throw new ArgumentException("Projection expression must select at least one property.", nameof(projection));
        }

        var attributeNames = new Dictionary<string, string>(propertyNames.Count);
        var sb = new StringBuilder();

        for (var i = 0; i < propertyNames.Count; i++)
        {
            var propertyName = propertyNames[i];
            var attributeName = DynamoDbMapper.GetPropertyAttributedName(typeof(T), propertyName);
            var placeholder = $"#proj{i}";

            attributeNames[placeholder] = attributeName;

            if (i > 0)
            {
                sb.Append(", ");
            }

            sb.Append(placeholder);
        }

        return (sb.ToString(), attributeNames);
    }

    private static List<string> ExtractPropertyNames(LambdaExpression expression)
    {
        var body = expression.Body;

        // Unwrap Convert(...) wrapper that the compiler adds for value types boxed to object
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            body = unary.Operand;
        }

        var names = new List<string>();

        switch (body)
        {
            // Anonymous type: x => new { x.Name, x.Status }
            case NewExpression newExpr when newExpr.Members != null:
            {
                foreach (var member in newExpr.Members)
                {
                    names.Add(member.Name);
                }

                break;
            }

            // Single property: x => x.Name (wrapped in Convert to object)
            case MemberExpression memberExpr:
            {
                names.Add(memberExpr.Member.Name);
                break;
            }

            // MemberInit: x => new SomeType { Name = x.Name }
            case MemberInitExpression initExpr:
            {
                foreach (var binding in initExpr.Bindings)
                {
                    names.Add(binding.Member.Name);
                }

                break;
            }

            default:
                throw new ArgumentException(
                    $"Unsupported projection expression type: {body.GetType().Name}. " +
                    "Use anonymous type projection like: x => new { x.Property1, x.Property2 }");
        }

        return names;
    }
}
