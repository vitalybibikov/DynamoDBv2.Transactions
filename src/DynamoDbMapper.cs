using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
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
        private static readonly ConcurrentDictionary<Type, int> PropertyCountCache = new();
        private static readonly ConcurrentDictionary<Type, string> RangeKeyCache = new();

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
        /// Gets or sets the global table name prefix applied to all table name resolutions.
        /// Set this at application startup (e.g., "dev-", "qa-", "prod-").
        /// </summary>
        public static string? TableNamePrefix { get; set; }

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
                return (TableNamePrefix ?? string.Empty) + mapping.TableName;
            }

            var tableAttribute = type.GetCustomAttribute<DynamoDBTableAttribute>();
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.TableName))
            {
                return (TableNamePrefix ?? string.Empty) + tableAttribute.TableName;
            }

            return (TableNamePrefix ?? string.Empty) + type.Name;
        }

        public static AttributeValue GetAttributeValue(int value) => new() { N = value.ToString(CultureInfo.InvariantCulture) };
        public static AttributeValue GetAttributeValue(long value) => new() { N = value.ToString(CultureInfo.InvariantCulture) };
        public static AttributeValue GetAttributeValue(decimal value) => new() { N = value.ToString(CultureInfo.InvariantCulture) };
        public static AttributeValue GetAttributeValue(float value) => new() { N = value.ToString(CultureInfo.InvariantCulture) };
        public static AttributeValue GetAttributeValue(double value) => new() { N = value.ToString(CultureInfo.InvariantCulture) };
        public static AttributeValue GetAttributeValue(string value) => new() { S = value };
        public static AttributeValue GetAttributeValue(char value) => new() { S = value.ToString() };
        public static AttributeValue GetAttributeValue(bool value) => new() { BOOL = value };
        public static AttributeValue GetAttributeValue(DateTime value) => new() { S = value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") };
        public static AttributeValue GetAttributeValue(DateTimeOffset value) => new() { S = value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffzzz") };

        public static AttributeValue? GetAttributeValue(object value)
        {
            return value switch
            {
                string s => GetAttributeValue(s),
                int i => GetAttributeValue(i),
                long l => GetAttributeValue(l),
                decimal d => GetAttributeValue(d),
                float f => GetAttributeValue(f),
                double dbl => GetAttributeValue(dbl),
                bool b => GetAttributeValue(b),
                char c => GetAttributeValue(c),
                DateTime dt => GetAttributeValue(dt),
                DateTimeOffset dto => new AttributeValue { S = dto.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffzzz") },
                Enum e => new AttributeValue { N = Convert.ToInt64(e).ToString(CultureInfo.InvariantCulture) },
                _ => ConvertToAttributeValueV2(value),
            };
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

        public static string GetRangeKeyAttributeName(Type type)
        {
            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                if (mapping.RangeKeyAttributeName != null)
                {
                    return mapping.RangeKeyAttributeName;
                }

                throw new ArgumentException("Type " + type.FullName + " does not have a range key attribute.");
            }

            return RangeKeyCache.GetOrAdd(type, static t =>
            {
                foreach (var property in GetCachedProperties(t))
                {
                    var rangeKeyAttr = property.GetCustomAttribute<DynamoDBRangeKeyAttribute>();
                    if (rangeKeyAttr != null)
                    {
                        return string.IsNullOrWhiteSpace(rangeKeyAttr.AttributeName)
                            ? property.Name
                            : rangeKeyAttr.AttributeName;
                    }
                }

                throw new ArgumentException("Failed to find range key attribute on type " + t.FullName);
            });
        }

        /// <summary>
        /// Tries to get the range key attribute name, returning null if no range key exists.
        /// </summary>
        /// <param name="type">The entity type.</param>
        /// <returns>The range key attribute name, or null if no range key is defined.</returns>
        public static string? TryGetRangeKeyAttributeName(Type type)
        {
            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                return mapping.RangeKeyAttributeName;
            }

            foreach (var property in GetCachedProperties(type))
            {
                var rangeKeyAttr = property.GetCustomAttribute<DynamoDBRangeKeyAttribute>();
                if (rangeKeyAttr != null)
                {
                    return string.IsNullOrWhiteSpace(rangeKeyAttr.AttributeName)
                        ? property.Name
                        : rangeKeyAttr.AttributeName;
                }
            }

            return null;
        }

        /// <summary>
        /// Deserializes a DynamoDB attribute dictionary back to a typed object.
        /// Uses source-generated mapping if available, otherwise falls back to reflection.
        /// </summary>
        /// <param name="type">The target entity type.</param>
        /// <param name="attributes">The DynamoDB attributes.</param>
        /// <returns>A new instance of the entity with properties populated from the attributes.</returns>
        public static object MapFromAttributes(Type type, Dictionary<string, AttributeValue> attributes)
        {
            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                return mapping.MapFromAttributes(attributes);
            }

            // Reflection fallback
            var instance = Activator.CreateInstance(type)
                ?? throw new InvalidOperationException($"Could not create instance of {type.FullName}. Ensure it has a parameterless constructor.");

            var properties = GetCachedProperties(type);
            foreach (var property in properties)
            {
                if (!property.CanWrite)
                {
                    continue;
                }

                var attributeName = GetPropertyAttributedName(type, property.Name);
                if (attributes.TryGetValue(attributeName, out var attrValue))
                {
                    var converted = ConvertFromAttributeValue(attrValue, property.PropertyType);
                    if (converted != null)
                    {
                        property.SetValue(instance, converted);
                    }
                }
            }

            return instance;
        }

        /// <summary>
        /// Typed deserialization helper.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="attributes">The DynamoDB attributes to deserialize.</param>
        /// <returns>A new instance of the entity.</returns>
        public static T MapFromAttributes<T>(Dictionary<string, AttributeValue> attributes)
            where T : class, new()
        {
            return (T)MapFromAttributes(typeof(T), attributes);
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

            if (obj is null)
            {
                return new Dictionary<string, AttributeValue>();
            }

            var type = obj.GetType();

            if (GeneratedMappings.TryGetValue(type, out var mapping))
            {
                // Source-generated code always produces V2 format;
                // fall through to reflection for explicit V1 requests.
                if (conversion == DynamoDBEntryConversion.V2)
                {
                    return mapping.MapToAttributes(obj, conversion);
                }
            }

            var properties = GetCachedProperties(type);
            var capacity = PropertyCountCache.GetOrAdd(type, static t => t.GetProperties().Length);
            var attributeMap = new Dictionary<string, AttributeValue>(capacity);

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
                        attributeMap[attributeName] = new AttributeValue { N = Convert.ToString(value, CultureInfo.InvariantCulture) };
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
            {
                var props = GetCachedProperties(t);
                for (var i = 0; i < props.Length; i++)
                {
                    if (props[i].GetCustomAttribute<DynamoDBVersionAttribute>() != null)
                    {
                        return props[i];
                    }
                }
                return null;
            });

            var value = versionProperty?.GetValue(item, null);

            if (versionProperty == null)
            {
                return (null, value);
            }

            return (GetPropertyAttributedName(type, versionProperty.Name), value);
        }

        /// <summary>
        /// Converts a DynamoDB <see cref="AttributeValue"/> to a CLR object of the specified type.
        /// Supports V1 and V2 attribute formats, including primitives, enums, sets, lists, maps, and nested objects.
        /// </summary>
        /// <param name="attrValue">The DynamoDB attribute value.</param>
        /// <param name="targetType">The target CLR type to convert to.</param>
        /// <returns>The converted value, or null if the attribute represents a NULL value.</returns>
        public static object? ConvertFromAttributeValue(AttributeValue attrValue, Type targetType)
        {
            if (attrValue.NULL == true)
            {
                return null;
            }

            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            if (attrValue.S != null)
            {
                if (underlyingType == typeof(string))
                {
                    return attrValue.S;
                }

                if (underlyingType == typeof(char) && attrValue.S.Length > 0)
                {
                    return attrValue.S[0];
                }

                if (underlyingType == typeof(DateTime))
                {
                    return DateTime.Parse(attrValue.S, CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal);
                }

                if (underlyingType == typeof(Guid))
                {
                    return Guid.Parse(attrValue.S);
                }

                if (underlyingType == typeof(DateTimeOffset))
                {
                    return DateTimeOffset.Parse(attrValue.S, CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None);
                }
            }

            if (attrValue.N != null)
            {
                if (underlyingType == typeof(int))
                {
                    return int.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(long))
                {
                    return long.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(decimal))
                {
                    return decimal.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(double))
                {
                    return double.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(float))
                {
                    return float.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(short))
                {
                    return short.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(byte))
                {
                    return byte.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(sbyte))
                {
                    return sbyte.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(ushort))
                {
                    return ushort.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(uint))
                {
                    return uint.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(ulong))
                {
                    return ulong.Parse(attrValue.N, CultureInfo.InvariantCulture);
                }

                if (underlyingType == typeof(bool))
                {
                    return attrValue.N != "0";
                }

                if (underlyingType.IsEnum)
                {
                    return Enum.ToObject(underlyingType, long.Parse(attrValue.N, CultureInfo.InvariantCulture));
                }
            }

            if (underlyingType == typeof(bool))
            {
                return attrValue.BOOL;
            }

            if (attrValue.B != null && underlyingType == typeof(byte[]))
            {
                return attrValue.B.ToArray();
            }

            if (attrValue.B != null && underlyingType == typeof(MemoryStream))
            {
                var ms = new MemoryStream();
                attrValue.B.CopyTo(ms);
                ms.Position = 0;
                return ms;
            }

            // Dictionary<string, T> from Map
            if (underlyingType.IsGenericType && underlyingType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
            {
                var args = underlyingType.GetGenericArguments();
                if (args[0] == typeof(string))
                {
                    var dict = (IDictionary)Activator.CreateInstance(underlyingType)!;
                    foreach (var kvp in attrValue.M)
                    {
                        dict[kvp.Key] = ConvertFromAttributeValue(kvp.Value, args[1]);
                    }
                    return dict;
                }
            }

            // HashSet<T> from SS/NS/BS
            if (underlyingType.IsGenericType && underlyingType.GetGenericTypeDefinition() == typeof(HashSet<>))
            {
                var elementType = underlyingType.GetGenericArguments()[0];

                if (elementType == typeof(string) && attrValue.SS?.Count > 0)
                {
                    return new HashSet<string>(attrValue.SS);
                }

                if (IsNumericType(elementType) && attrValue.NS?.Count > 0)
                {
                    var set = Activator.CreateInstance(underlyingType)!;
                    var addMethod = underlyingType.GetMethod("Add")!;
                    foreach (var n in attrValue.NS)
                    {
                        addMethod.Invoke(set, new[] { Convert.ChangeType(decimal.Parse(n, CultureInfo.InvariantCulture), elementType, CultureInfo.InvariantCulture) });
                    }
                    return set;
                }

                if (attrValue.BS?.Count > 0)
                {
                    if (elementType == typeof(byte[]))
                    {
                        var set = new HashSet<byte[]>();
                        foreach (var ms in attrValue.BS)
                        {
                            set.Add(ms.ToArray());
                        }
                        return set;
                    }

                    if (elementType == typeof(MemoryStream))
                    {
                        var set = new HashSet<MemoryStream>();
                        foreach (var ms in attrValue.BS)
                        {
                            var copy = new MemoryStream();
                            ms.CopyTo(copy);
                            copy.Position = 0;
                            set.Add(copy);
                        }
                        return set;
                    }
                }
            }

            // List<T> / IList<T> / ICollection<T> / IEnumerable<T> from L, SS, or NS
            if (underlyingType.IsGenericType)
            {
                var genericDef = underlyingType.GetGenericTypeDefinition();
                if (genericDef == typeof(List<>) || genericDef == typeof(IList<>) ||
                    genericDef == typeof(ICollection<>) || genericDef == typeof(IEnumerable<>))
                {
                    var elementType = underlyingType.GetGenericArguments()[0];

                    // V1 fallback: SS (string sets) — check before L since L is always initialized
                    if (elementType == typeof(string) && attrValue.SS?.Count > 0)
                    {
                        return new List<string>(attrValue.SS);
                    }

                    // V1 fallback: NS (number sets)
                    if (IsNumericType(elementType) && attrValue.NS?.Count > 0)
                    {
                        var listType = typeof(List<>).MakeGenericType(elementType);
                        var list = (IList)Activator.CreateInstance(listType)!;
                        foreach (var n in attrValue.NS)
                        {
                            list.Add(Convert.ChangeType(decimal.Parse(n, CultureInfo.InvariantCulture), elementType, CultureInfo.InvariantCulture));
                        }
                        return list;
                    }

                    // V2 format: L (list of AttributeValues) — always process, may be empty
                    {
                        var listType = typeof(List<>).MakeGenericType(elementType);
                        var list = (IList)Activator.CreateInstance(listType)!;
                        foreach (var item in attrValue.L)
                        {
                            list.Add(ConvertFromAttributeValue(item, elementType));
                        }
                        return list;
                    }
                }
            }

            // Array from List
            if (underlyingType.IsArray)
            {
                var elementType = underlyingType.GetElementType()!;
                var array = Array.CreateInstance(elementType, attrValue.L.Count);
                for (int i = 0; i < attrValue.L.Count; i++)
                {
                    array.SetValue(ConvertFromAttributeValue(attrValue.L[i], elementType), i);
                }
                return array;
            }

            // Nested class/record from Map
            if (underlyingType.IsClass && underlyingType != typeof(string) && attrValue.M.Count > 0)
            {
                return MapFromAttributes(underlyingType, attrValue.M);
            }

            return null;
        }

        private static PropertyInfo[] GetCachedProperties(Type type)
        {
            return PropertyCache.GetOrAdd(type, static t => t.GetProperties()
                .Where(p => p.GetCustomAttribute<DynamoDBIgnoreAttribute>() == null)
                .ToArray());
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
                    return new AttributeValue { N = Convert.ToString(value, CultureInfo.InvariantCulture) };
                case DateTime dateTime:
                    return new AttributeValue { S = dateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") };
                case Guid guid:
                    return new AttributeValue { S = guid.ToString() };
                case DateTimeOffset dto:
                    return new AttributeValue { S = dto.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffzzz") };
                case Enum e:
                    return new AttributeValue { N = Convert.ToInt64(e).ToString(CultureInfo.InvariantCulture) };
                case IDictionary dictionary:
                    return new AttributeValue { M = MapDictionaryToAttributeValue(dictionary, ConvertToAttributeValueV2) };
                case IEnumerable enumerable:
                    {
                        var type = value.GetType();
                        var isHashSet = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(HashSet<>);
                        var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();

                        if (isHashSet)
                        {
                            // DynamoDB does not allow empty sets (SS/NS/BS)
                            if (enumerable is ICollection emptyCheck && emptyCheck.Count == 0)
                            {
                                return new AttributeValue { NULL = true };
                            }

                            if (elementType != null)
                            {
                                if (IsNumericType(elementType))
                                {
                                    var nsList = new List<string>();
                                    foreach (object e in enumerable)
                                    {
                                        nsList.Add(Convert.ToString(e, CultureInfo.InvariantCulture)!);
                                    }
                                    return new AttributeValue { NS = nsList };
                                }
                                else if (elementType == typeof(string) || elementType == typeof(char) || elementType == typeof(Guid) || elementType == typeof(DateTime))
                                {
                                    var ssList = new List<string>();
                                    foreach (object e in enumerable)
                                    {
                                        ssList.Add(e is DateTime dt ? dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") : e.ToString()!);
                                    }
                                    return new AttributeValue { SS = ssList };
                                }
                                else if (elementType == typeof(byte[]) || elementType == typeof(MemoryStream))
                                {
                                    var bsList = new List<MemoryStream>();
                                    foreach (object e in enumerable)
                                    {
                                        if (e is byte[] b)
                                        {
                                            bsList.Add(new MemoryStream(b));
                                        }
                                        else if (e is MemoryStream ms)
                                        {
                                            if (ms.Position != 0 && ms.CanSeek)
                                            {
                                                ms.Seek(0, SeekOrigin.Begin);
                                            }
                                            bsList.Add(ms);
                                        }
                                        else
                                        {
                                            throw new ArgumentException("Invalid binary element type in HashSet.");
                                        }
                                    }
                                    return new AttributeValue { BS = bsList };
                                }
                            }
                            throw new ArgumentException($"Unsupported HashSet element type: {elementType}");
                        }
                        else
                        {
                            var list = new List<AttributeValue>();
                            foreach (object e in enumerable)
                            {
                                list.Add(ConvertToAttributeValueV2(e));
                            }
                            return new AttributeValue { L = list };
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
                    return new AttributeValue { N = Convert.ToString(value, CultureInfo.InvariantCulture) };
                case DateTime dateTime:
                    return new AttributeValue { S = dateTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") };
                case Guid guid:
                    return new AttributeValue { S = guid.ToString() };
                case DateTimeOffset dto:
                    return new AttributeValue { S = dto.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffzzz") };
                case Enum e:
                    return new AttributeValue { N = Convert.ToInt64(e).ToString(CultureInfo.InvariantCulture) };
                case IDictionary dictionary:
                    return new AttributeValue { M = MapDictionaryToAttributeValue(dictionary, ConvertToAttributeValueV1) };
                case IEnumerable enumerable:
                    var type = value.GetType();
                    var elementType = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();

                    if (elementType != null)
                    {
                        if (elementType == typeof(bool))
                        {
                            var boolNsList = new List<string>();
                            foreach (bool b in enumerable)
                            {
                                boolNsList.Add(b ? "1" : "0");
                            }
                            return boolNsList.Count > 0
                                ? new AttributeValue { NS = boolNsList }
                                : new AttributeValue { NULL = true };
                        }
                        else if (IsNumericType(elementType))
                        {
                            var nsList = new List<string>();
                            foreach (object e in enumerable)
                            {
                                nsList.Add(Convert.ToString(e, CultureInfo.InvariantCulture)!);
                            }
                            return nsList.Count > 0
                                ? new AttributeValue { NS = nsList }
                                : new AttributeValue { NULL = true };
                        }
                        else if (IsNumericNullableType(elementType))
                        {
                            var nullableList = new List<AttributeValue>();
                            foreach (object e in enumerable)
                            {
                                nullableList.Add(ConvertToAttributeValueV2(e));
                            }
                            return new AttributeValue { L = nullableList };
                        }
                        else if (elementType == typeof(string) || elementType == typeof(char) || elementType == typeof(Guid) || elementType == typeof(DateTime))
                        {
                            var ssList = new List<string>();
                            foreach (object e in enumerable)
                            {
                                ssList.Add(e is DateTime dt ? dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") : e.ToString()!);
                            }
                            return ssList.Count > 0
                                ? new AttributeValue { SS = ssList }
                                : new AttributeValue { NULL = true };
                        }
                        else if (elementType == typeof(byte[]) || elementType == typeof(MemoryStream))
                        {
                            var bsList = new List<MemoryStream>();
                            foreach (object e in enumerable)
                            {
                                if (e is byte[] b)
                                {
                                    bsList.Add(new MemoryStream(b));
                                }
                                else if (e is MemoryStream ms)
                                {
                                    if (ms.Position != 0 && ms.CanSeek)
                                    {
                                        ms.Seek(0, SeekOrigin.Begin);
                                    }
                                    bsList.Add(ms);
                                }
                                else
                                {
                                    throw new ArgumentException("Invalid binary element type in collection.");
                                }
                            }
                            return bsList.Count > 0
                                ? new AttributeValue { BS = bsList }
                                : new AttributeValue { NULL = true };
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
