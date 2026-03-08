using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions
{
    public static class DynamoDbMapper
    {
        private static readonly ConcurrentDictionary<Type, IGeneratedTypeMapping> GeneratedMappings = new();
        private static readonly ConcurrentDictionary<(Type, string), string> PropertyNameCache = new();
        private static readonly ConcurrentDictionary<Type, string> HashKeyCache = new();
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertyCache = new();
        private static readonly ConcurrentDictionary<Type, PropertyInfo?> VersionPropertyCache = new();

        /// <summary>
        /// Registers a source-generated mapping for the specified type.
        /// Called automatically by generated [ModuleInitializer] code.
        /// </summary>
        /// <typeparam name="T">The DynamoDB entity type.</typeparam>
        /// <param name="mapping">The generated mapping instance.</param>
        public static void RegisterGeneratedMapping<T>(IGeneratedTypeMapping mapping)
            where T : class
        {
            GeneratedMappings[typeof(T)] = mapping ?? throw new ArgumentNullException(nameof(mapping));
        }

        /// <summary>
        /// Gets the DynamoDB table name for the specified type,
        /// using the generated mapping if available, otherwise falling back to reflection.
        /// </summary>
        /// <param name="type">The entity type.</param>
        /// <returns>The DynamoDB table name.</returns>
        public static string GetTableName(Type type)
        {
            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                return mapping.TableName;
            }

            var tableAttribute = type.GetCustomAttribute<DynamoDBTableAttribute>();
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.TableName))
            {
                return tableAttribute.TableName;
            }

            return type.Name;
        }

        public static AttributeValue? GetAttributeValue(object value)
        {
            var attribute = ConvertToAttributeValueV2(value);
            return attribute;
        }

        public static string GetPropertyAttributedName(Type type, string propertyName)
        {
            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                return mapping.GetPropertyAttributeName(propertyName);
            }

            return PropertyNameCache.GetOrAdd((type, propertyName), static key =>
            {
                var (t, pName) = key;
                var property = t.GetProperty(pName);

                if (property == null)
                {
                    throw new ArgumentException($"Property {pName} not found on type {t.Name}");
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
            });
        }

        public static string GetHashKeyAttributeName(Type type)
        {
            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                return mapping.HashKeyAttributeName;
            }

            return HashKeyCache.GetOrAdd(type, static t =>
            {
                foreach (var property in GetCachedProperties(t))
                {
                    var propertyVal = property.GetCustomAttribute<DynamoDBHashKeyAttribute>();
                    if (propertyVal != null)
                    {
                        return string.IsNullOrWhiteSpace(propertyVal.AttributeName)
                            ? property.Name
                            : propertyVal.AttributeName;
                    }
                }

                throw new ArgumentException("Failed to find hash key attribute on type " + t.FullName);
            });
        }

        /// <summary>
        /// Maps to attributes, defaulting to V2 conversion schema, if otherwise not specified.
        /// </summary>
        /// <param name="obj">Object to map.</param>
        /// <param name="conversion">Conversion Type, defaults to V2</param>
        /// <returns>Attributes dictionary to return.</returns>
        public static Dictionary<string, AttributeValue> MapToAttribute(object? obj, DynamoDBEntryConversion? conversion = null)
        {
            conversion ??= DynamoDBEntryConversion.V2;

            var attributeMap = new Dictionary<string, AttributeValue>();

            if (obj is null)
            {
                return attributeMap;
            }

            var type = obj.GetType();

            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                return mapping.MapToAttributes(obj, conversion);
            }

            var properties = GetCachedProperties(type);

            foreach (var property in properties)
            {
                if (!property.CanRead)
                {
                    continue;
                }

                var attributeName = GetPropertyAttributedName(type, property.Name);
                var value = property.GetValue(obj, null);

                if (property.GetCustomAttribute<DynamoDBVersionAttribute>() != null)
                {
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

            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                return mapping.GetVersion(item);
            }

            var versionProperty = VersionPropertyCache.GetOrAdd(type, static t =>
                GetCachedProperties(t)
                    .FirstOrDefault(x => x.GetCustomAttribute<DynamoDBVersionAttribute>() != null));

            var value = versionProperty?.GetValue(item, null);

            return (versionProperty?.Name, value);
        }

        private static PropertyInfo[] GetCachedProperties(Type type)
        {
            return PropertyCache.GetOrAdd(type, static t => t.GetProperties());
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
                    return new AttributeValue { M = MapDictionaryToAttributeValue(dictionary, ConvertToAttributeValueV2) };
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
                    return new AttributeValue { M = MapDictionaryToAttributeValue(dictionary, ConvertToAttributeValueV1) };
                case IEnumerable enumerable:
                    var type = value.GetType();
                    var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();

                    if (elementType != null)
                    {
                        if (elementType == typeof(bool))
                        {
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

        private static Dictionary<string, AttributeValue> MapDictionaryToAttributeValue(IDictionary dictionary, Func<object?, AttributeValue> converter)
        {
            var attributeValues = new Dictionary<string, AttributeValue>(dictionary.Count);
            foreach (DictionaryEntry entry in dictionary)
            {
                attributeValues[entry.Key.ToString()!] = converter(entry.Value!);
            }
            return attributeValues;
        }
    }
}
