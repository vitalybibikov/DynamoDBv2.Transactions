using System.Collections;
using System.Reflection;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions
{
    public static class DynamoDbMapper
    {
        public static Dictionary<string, AttributeValue> MapToAttributeValueDictionary(this ITransactional obj)
        {
            return MapToAttribute(obj);
        }

        public static AttributeValue? GetAttributeValue(object value)
        {
            var attribute = ConvertToAttributeValue(value);
            return attribute;
        }

        public static string GetPropertyAttributedName(Type type, string propertyName)
        {
            var property = type.GetProperty(propertyName);

            if (property == null)
            {
                throw new ArgumentException($"Property {propertyName} not found on type {type.Name}");
            }

            var attributeName = property.Name;

            var hashKeyAttr = property.GetCustomAttribute<DynamoDBHashKeyAttribute>();
            if (hashKeyAttr != null && !string.IsNullOrWhiteSpace(hashKeyAttr.AttributeName))
            {
                attributeName = hashKeyAttr.AttributeName;
            }
            else
            {
                var rangeKeyAttr = property.GetCustomAttribute<DynamoDBRangeKeyAttribute>();
                if (rangeKeyAttr != null && !string.IsNullOrWhiteSpace(rangeKeyAttr.AttributeName))
                {
                    attributeName = rangeKeyAttr.AttributeName;
                }
                else
                {
                    var propertyAttr = property.GetCustomAttribute<DynamoDBPropertyAttribute>();
                    if (propertyAttr != null && !string.IsNullOrWhiteSpace(propertyAttr.AttributeName))
                    {
                        attributeName = propertyAttr.AttributeName;
                    }
                }
            }

            return attributeName;
        }

        public static string GetHashKeyAttributeName(Type type)
        {
            foreach (var property in type.GetProperties())
            {
                var propertyVal = property.GetCustomAttribute<DynamoDBHashKeyAttribute>();
                if (propertyVal != null)
                {
                    return propertyVal.AttributeName;
                }
            }

            throw new ArgumentException("Failed to find hash key attribute on type " + type.FullName);
        }

        public static Dictionary<string, AttributeValue> MapToAttribute(object? obj)
        {
            var attributeMap = new Dictionary<string, AttributeValue>();

            if (obj is null)
            {
                return attributeMap;
            }

            var type = obj.GetType();
            foreach (var property in type.GetProperties())
            {
                // Skip properties that cannot be read.
                if (!property.CanRead)
                {
                    continue;
                }

                // Default attribute name is the property name.
                var attributeName = GetPropertyAttributedName(type, property.Name);
                var value = property.GetValue(obj, null);

                if (property.GetCustomAttribute<DynamoDBVersionAttribute>() != null)
                {
                    // For version attribute, ensure it is a numeric type; otherwise, ignore.
                    if (value != null && (value is int || value is long || value is decimal))
                    {
                        attributeMap[attributeName] = new AttributeValue { N = value.ToString() };
                    }
                    else
                    {
                        var valueType = property.PropertyType;

                        if (value == null && (valueType == typeof(int?) || valueType == typeof(long?) || valueType == typeof(decimal?)))
                        {
                            attributeMap[attributeName] = new AttributeValue { NULL = true };
                        }
                    }
                }
                else if (value != null)
                {
                    attributeMap[attributeName] = ConvertToAttributeValue(value);
                }
            }

            return attributeMap;
        }

        public static (string? VersionProperty, object? Value) GetVersion<T>(T item)
        {
            if (item is null)
            {
                return (null, null);
            }

            var type = item.GetType();

            var versionProperty = type
                .GetProperties()
                .FirstOrDefault(x => x.GetCustomAttribute<DynamoDBVersionAttribute>() != null);

            var value = versionProperty?.GetValue(item, null);

            return (versionProperty?.Name, value);
        }

        private static AttributeValue ConvertToAttributeValue(object? value)
        {
            if (value is null)
            {
                return new AttributeValue { NULL = true };
            }

            switch (value)
            {
                case string s:
                    return new AttributeValue { S = s };
                case bool b:
                    return new AttributeValue { BOOL = b };
                case byte[] bytes:
                    return new AttributeValue { B = new MemoryStream(bytes) };
                case MemoryStream memoryStream:
                    if (memoryStream.Position != 0 && memoryStream.CanSeek)
                    {
                        memoryStream.Seek(0, SeekOrigin.Begin);
                    }
                    return new AttributeValue { B = memoryStream };
                case decimal:
                case float:
                case double:
                case long:
                case int:
                    return new AttributeValue { N = value.ToString() };
                case DateTime dateTime:
                    //ISO 8601 format
                    return new AttributeValue { S = dateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") };
                case IList list:
                    return new AttributeValue { L = MapListToAttributeValue(list) };
                case IDictionary dictionary:
                    return new AttributeValue { M = MapDictionaryToAttributeValue(dictionary) };
                default:

                    var isStruct = value.GetType().IsValueType && !value.GetType().IsPrimitive;
                    if (value.GetType().IsClass || (value.GetType().IsValueType && !value.GetType().IsPrimitive && !value.GetType().IsEnum))
                    {
                        var attribute = MapToAttribute(value);

                        if (isStruct)
                        {
                            throw new ArgumentException($"Unsupported type: {value.GetType()}");
                        }

                        return new AttributeValue { M = attribute };
                    }

                    throw new ArgumentException($"Unsupported type: {value.GetType()}");
            }
        }

        private static List<AttributeValue> MapListToAttributeValue(IList list)
        {
            return (from object? item in list select ConvertToAttributeValue(item)).ToList();
        }

        private static Dictionary<string, AttributeValue> MapDictionaryToAttributeValue(IDictionary dictionary)
        {
            var attributeValues = new Dictionary<string, AttributeValue>();
            foreach (DictionaryEntry entry in dictionary)
            {
                attributeValues[entry.Key.ToString()!] = ConvertToAttributeValue(entry.Value!);
            }
            return attributeValues;
        }
    }
}
