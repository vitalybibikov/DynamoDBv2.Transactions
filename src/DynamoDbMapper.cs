using System.Collections;
using System.Reflection;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions
{
    public static class DynamoDbMapper
    {
        public static AttributeValue? GetAttributeValue(object value)
        {
            var attribute = ConvertToAttributeValueV2(value);
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

        /// <summary>
        /// Maps to attributes, defaulting to V2 conversion schema, if otherwise not specified.
        /// </summary>
        /// <param name="obj">Object to map.</param>
        /// <param name="conversion">Conversion Type, defaults to V2</param>
        /// <returns>Attributes dictionary to return.</returns>
        public static Dictionary<string, AttributeValue> MapToAttribute(object? obj, DynamoDBEntryConversion? conversion = null)
        {
            if (conversion == null)
            {
                conversion = DynamoDBEntryConversion.V2;
            }

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
                    if (conversion == DynamoDBEntryConversion.V2)
                    {
                        attributeMap[attributeName] = ConvertToAttributeValueV2(value);
                    }
                    else
                    {
                        attributeMap[attributeName] = ConvertToAttributeValueV1(value);
                    }
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

        private static AttributeValue ConvertToAttributeValueV2(object? value)
        {
            if (value is null)
            {
                return new AttributeValue { NULL = true };
            }

            switch (value)
            {
                case string s:
                    return new AttributeValue { S = s };
                case char c:
                    return new AttributeValue { S = c.ToString() };
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
                case decimal or float or double or byte or long or int or short or uint or ulong or ushort or sbyte:
                    return new AttributeValue { N = Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) };
                case DateTime dateTime:
                    return new AttributeValue { S = dateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") };
                case Guid guid:
                    return new AttributeValue { S = guid.ToString() };
                case IDictionary dictionary:
                    return new AttributeValue { M = MapDictionaryToAttributeValue(dictionary) };
                case IEnumerable enumerable:
                    {
                        var type = value.GetType();
                        var isHashSet = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(HashSet<>);
                        var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();

                        if (isHashSet)
                        {
                            if (elementType != null)
                            {
                                if (IsNumericType(elementType))
                                {
                                    return new AttributeValue
                                    {
                                        NS = enumerable.Cast<object>()
                                                       .Select(e => Convert.ToString(e, System.Globalization.CultureInfo.InvariantCulture))
                                                       .ToList()
                                    };
                                }
                                else if (elementType == typeof(string) || elementType == typeof(char) || elementType == typeof(Guid) || elementType == typeof(DateTime))
                                {
                                    return new AttributeValue
                                    {
                                        SS = enumerable.Cast<object>()
                                                       .Select(e => e is DateTime dt ? dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") : e.ToString())
                                                       .ToList()
                                    };
                                }
                                else if (elementType == typeof(byte[]) || elementType == typeof(MemoryStream))
                                {
                                    return new AttributeValue
                                    {
                                        BS = enumerable.Cast<object>()
                                                       .Select(e =>
                                                       {
                                                           if (e is byte[] b)
                                                           {
                                                               return new MemoryStream(b);
                                                           }

                                                           if (e is MemoryStream ms)
                                                           {
                                                               if (ms.Position != 0 && ms.CanSeek)
                                                               {
                                                                   ms.Seek(0, SeekOrigin.Begin);
                                                               }

                                                               return ms;
                                                           }
                                                           throw new ArgumentException("Invalid binary element type in HashSet.");
                                                       })
                                                       .ToList()
                                    };
                                }
                            }
                            throw new ArgumentException($"Unsupported HashSet element type: {elementType}");
                        }
                        else
                        {
                            // For List<T> and arrays, always map to L
                            return new AttributeValue
                            {
                                L = enumerable.Cast<object>()
                                             .Select(ConvertToAttributeValueV2)
                                             .ToList()
                            };
                        }
                    }
                default:
                    var isStruct = value.GetType().IsValueType && !value.GetType().IsPrimitive;

                    if (value.GetType().IsClass || isStruct)
                    {
                        if (isStruct)
                        {
                            throw new ArgumentException($"Unsupported type: {value.GetType()}");
                        }

                        var attribute = MapToAttribute(value, DynamoDBEntryConversion.V2);

                        return new AttributeValue { M = attribute };
                    }

                    throw new ArgumentException($"Unsupported type: {value.GetType()}");
            }
        }

        private static AttributeValue ConvertToAttributeValueV1(object? value)
        {
            if (value is null)
            {
                return new AttributeValue { NULL = true };
            }

            switch (value)
            {
                case string s:
                    return new AttributeValue { S = s };
                case char c:
                    return new AttributeValue { S = c.ToString() };
                case bool b:
                    return new AttributeValue { N = b ? "1" : "0" };
                case byte[] bytes:
                    return new AttributeValue { B = new MemoryStream(bytes) };
                case MemoryStream memoryStream:
                    if (memoryStream.Position != 0 && memoryStream.CanSeek)
                    {
                        memoryStream.Seek(0, SeekOrigin.Begin);
                    }
                    return new AttributeValue { B = memoryStream };
                case decimal or float or double or byte or long or int or short or uint or ulong or ushort or sbyte:
                    return new AttributeValue { N = Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) };
                case DateTime dateTime:
                    return new AttributeValue { S = dateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") };
                case Guid guid:
                    return new AttributeValue { S = guid.ToString() };
                case IDictionary dictionary:
                    return new AttributeValue { M = MapDictionaryToAttributeValue(dictionary) };
                case IEnumerable enumerable:
                    var type = value.GetType();
                    var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();

                    if (elementType != null)
                    {
                        if (elementType == typeof(bool))
                        {
                            // Handle bool[] as NS with 0/1
                            return new AttributeValue
                            {
                                NS = enumerable.Cast<bool>()
                                    .Select(b => b ? "1" : "0")
                                    .ToList()
                            };
                        }
                        else if (IsNumericType(elementType))
                        {
                            return new AttributeValue
                            {
                                NS = enumerable.Cast<object>()
                                               .Select(e => Convert.ToString(e, System.Globalization.CultureInfo.InvariantCulture))
                                               .ToList()
                            };
                        }
                        //if array contains nullables, then dynamo foces to use V2.
                        else if (IsNumericNullableType(elementType))
                        {
                            return new AttributeValue
                            {
                                L = enumerable.Cast<object>()
                                    .Select(ConvertToAttributeValueV2)
                                    .ToList()
                            };
                        }
                        else if (elementType == typeof(string) || elementType == typeof(char) || elementType == typeof(Guid) || elementType == typeof(DateTime))
                        {
                            return new AttributeValue
                            {
                                SS = enumerable.Cast<object>()
                                               .Select(e => e is DateTime dt ? dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") : e.ToString())
                                               .ToList()
                            };
                        }
                        else if (elementType == typeof(byte[]) || elementType == typeof(MemoryStream))
                        {
                            return new AttributeValue
                            {
                                BS = enumerable.Cast<object>()
                                               .Select(e =>
                                               {
                                                   if (e is byte[] b)
                                                   {
                                                       return new MemoryStream(b);
                                                   }
                                                   if (e is MemoryStream ms)
                                                   {
                                                       if (ms.Position != 0 && ms.CanSeek)
                                                       {
                                                           ms.Seek(0, SeekOrigin.Begin);
                                                       }
                                                       return ms;
                                                   }
                                                   throw new ArgumentException("Invalid binary element type in collection.");
                                               })
                                               .ToList()
                            };
                        }
                    }
                    throw new ArgumentException($"Unsupported collection type: {value.GetType()}");

                default:
                    var isStruct = value.GetType().IsValueType && !value.GetType().IsPrimitive;

                    if (value.GetType().IsClass || isStruct)
                    {
                        var attribute = MapToAttribute(value, DynamoDBEntryConversion.V1);

                        if (isStruct)
                        {
                            throw new ArgumentException($"Unsupported type: {value.GetType()}");
                        }

                        return new AttributeValue { M = attribute };
                    }

                    throw new ArgumentException($"Unsupported type: {value.GetType()}");
            }
        }

        private static bool IsNumericType(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte) ||
                   type == typeof(short) || type == typeof(ushort) ||
                   type == typeof(int) || type == typeof(uint) ||
                   type == typeof(long) || type == typeof(ulong) ||
                   type == typeof(float) || type == typeof(double) ||
                   type == typeof(decimal);
        }

        private static bool IsNumericNullableType(Type type)
        {
            return type == typeof(byte?) || type == typeof(sbyte?) ||
                   type == typeof(short?) || type == typeof(ushort?) ||
                   type == typeof(int?) || type == typeof(uint?) ||
                   type == typeof(long?) || type == typeof(ulong?) ||
                   type == typeof(float?) || type == typeof(double?) ||
                   type == typeof(decimal?);
        }

        private static Dictionary<string, AttributeValue> MapDictionaryToAttributeValue(IDictionary dictionary)
        {
            var attributeValues = new Dictionary<string, AttributeValue>();
            foreach (DictionaryEntry entry in dictionary)
            {
                attributeValues[entry.Key.ToString()!] = ConvertToAttributeValueV2(entry.Value!);
            }
            return attributeValues;
        }
    }
}
