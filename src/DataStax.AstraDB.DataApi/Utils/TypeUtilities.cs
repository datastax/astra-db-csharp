

using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Utils;

/// <summary>
/// Utility methods for mapping .NET types to Data API column type descriptors.
/// </summary>
public class TypeUtilities
{
    internal static Type GetUnderlyingType(Type propertyType, int dictionaryPosition = 1)
    {
        if (propertyType.IsGenericType)
        {
            if (propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return Nullable.GetUnderlyingType(propertyType);
            }
            Type genericTypeDefinition = propertyType.GetGenericTypeDefinition();
            Type[] genericArguments = propertyType.GetGenericArguments();

            if (genericArguments.Length > 1)
            {
                return genericArguments[dictionaryPosition];
            }

            return genericArguments[0];
        }
        if (propertyType.IsArray)
        {
            return propertyType.GetElementType();
        }
        return propertyType;
    }

    /// <summary>
    /// Returns the <see cref="DataApiType"/> that corresponds to the given .NET type,
    /// unwrapping <c>Nullable&lt;T&gt;</c> if necessary.
    /// </summary>
    public static DataApiType GetDataApiType(Type propertyType)
    {
        Type underlyingType;
        if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            underlyingType = Nullable.GetUnderlyingType(propertyType);
        }
        else
        {
            underlyingType = propertyType;
        }
        return GetDataApiTypeFromUnderlyingType(underlyingType);
    }

    /// <summary>
    /// Returns the <see cref="DataApiType"/> that corresponds to the given non-nullable .NET type.
    /// </summary>
    public static DataApiType GetDataApiTypeFromUnderlyingType(Type propertyType)
    {
        DataApiType type = null;
        switch (Type.GetTypeCode(propertyType))
        {
            case TypeCode.Int32:
            case TypeCode.Int16:
            case TypeCode.Byte:
                return DataApiType.Int();
            case TypeCode.String:
                return DataApiType.Text();
            case TypeCode.Boolean:
                return DataApiType.Boolean();
            case TypeCode.DateTime:
                return DataApiType.Timestamp();
            case TypeCode.Decimal:
                return DataApiType.Decimal();
            case TypeCode.Double:
                return DataApiType.Double();
            case TypeCode.Int64:
                return DataApiType.BigInt();
            case TypeCode.Single:
                return DataApiType.Float();
            case TypeCode.Object:
                if (propertyType.FullName == "System.DateOnly")
                {
                    return DataApiType.Date();
                }
                else if (propertyType.FullName == "System.TimeOnly")
                {
                    return DataApiType.Time();
                }
                else if (propertyType.IsArray)
                {
                    Type elementType = propertyType.GetElementType();
                    if (elementType == typeof(byte))
                    {
                        return DataApiType.Blob();
                    }
                    else
                    {
                        return DataApiType.List(GetDataApiTypeFromUnderlyingType(elementType));
                    }
                }
                else if (propertyType.IsEnum)
                {
                    //skip
                    Console.WriteLine($"Enum types are not currently supported for column. Consider using a string or int property instead.");
                }
                else if (propertyType == typeof(Guid))
                {
                    return DataApiType.Uuid();
                }
                else if (propertyType == typeof(Duration))
                {
                    return DataApiType.Duration();
                }
                else if (propertyType == typeof(IPAddress))
                {
                    return DataApiType.Inet();
                }
                else if (propertyType.IsGenericType)
                {
                    Type genericTypeDefinition = propertyType.GetGenericTypeDefinition();
                    Type[] genericArguments = propertyType.GetGenericArguments();

                    if (genericTypeDefinition == typeof(Dictionary<,>))
                    {
                        if (genericArguments.Length == 2)
                        {
                            return DataApiType.Map(GetDataApiTypeFromUnderlyingType(genericArguments[0]), GetDataApiTypeFromUnderlyingType(genericArguments[1]));
                        }
                    }
                    else if (genericTypeDefinition == typeof(List<>))
                    {
                        return DataApiType.List(GetDataApiTypeFromUnderlyingType(genericArguments[0]));
                    }
                    else if (genericTypeDefinition == typeof(HashSet<>))
                    {
                        return DataApiType.Set(GetDataApiTypeFromUnderlyingType(genericArguments[0]));
                    }
                    else
                    {
                        //skip
                        Console.WriteLine($"Warning: Unhandled generic type: {propertyType.Name}");
                    }
                }
                else
                {
                    var attribute = propertyType.GetCustomAttribute<UserDefinedTypeAttribute>();
                    if (attribute != null)
                    {
                        var typeName = UserDefinedTypeRequest.GetUserDefinedTypeName(propertyType, attribute);
                        return DataApiType.UserDefined(typeName);
                    }
                    //skip
                    Console.WriteLine($"Warning: Unhandled type: {propertyType.Name}");
                }
                break;
            default:
                //skip
                Console.WriteLine($"Warning: Unhandled type code: {Type.GetTypeCode(propertyType)}");
                break;
        }
        return type;
    }

    internal static IEnumerable<UserDefinedProperty> FindPropertiesWithUserDefinedTypeAttribute(Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var property in properties)
        {
            var attribute = property.PropertyType.GetCustomAttribute<UserDefinedTypeAttribute>();
            if (attribute != null)
            {
                yield return new UserDefinedProperty() { Property = property, Attribute = attribute, UnderlyingType = GetUnderlyingType(property.PropertyType) };
                continue;
            }

            Type elementType = GetCollectionElementType(property.PropertyType);
            attribute = elementType == null ? null : elementType.GetCustomAttribute<UserDefinedTypeAttribute>();
            if (elementType != null && attribute != null)
            {
                yield return new UserDefinedProperty() { Property = property, Attribute = attribute, UnderlyingType = elementType };
            }
        }
    }

    private static Type GetCollectionElementType(Type type)
    {
        if (type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(type))
        {
            if (type.IsGenericType)
            {
                var genericType = type.GetGenericTypeDefinition();
                if (genericType == typeof(List<>) ||
                    genericType == typeof(HashSet<>) ||
                    genericType == typeof(Dictionary<,>) ||
                    genericType == typeof(IEnumerable<>) ||
                    genericType == typeof(ICollection<>) ||
                    genericType == typeof(IList<>))
                {
                    if (genericType == typeof(Dictionary<,>))
                    {
                        return type.GetGenericArguments()[1]; // Value type
                    }
                    return type.GetGenericArguments()[0];
                }
            }
            else if (type.IsArray)
            {
                return type.GetElementType();
            }
        }
        return null;
    }

}

internal class UserDefinedProperty
{
    public PropertyInfo Property { get; set; }
    public UserDefinedTypeAttribute Attribute { get; set; }
    public Type UnderlyingType { get; set; }
}

/// <summary>
/// Represents a Data API column type (e.g., text, int, uuid, vector).
/// </summary>
public class DataApiType
{
    /// <summary>
    /// Initializes a new <see cref="DataApiType"/> with the given type key.
    /// </summary>
    public DataApiType(string key)
    {
        Key = key;
    }

    /// <summary>
    /// The Data API type identifier string (e.g., "text", "int", "uuid").
    /// </summary>
    [JsonPropertyName("type")]
    public string Key { get; set; }

    internal virtual object AsValueType => Key;

    internal virtual object AsColumnType => this;

    internal virtual bool IsSimpleType => true;

    /// <summary>Creates an ascii column type.</summary>
    public static DataApiType Ascii() => new DataApiType("ascii");
    /// <summary>Creates a bigint column type.</summary>
    public static DataApiType BigInt() => new DataApiType("bigint");
    /// <summary>Creates a blob column type.</summary>
    public static DataApiType Blob() => new DataApiType("blob");
    /// <summary>Creates a boolean column type.</summary>
    public static DataApiType Boolean() => new DataApiType("boolean");
    /// <summary>Creates a date column type.</summary>
    public static DataApiType Date() => new DataApiType("date");
    /// <summary>Creates a decimal column type.</summary>
    public static DataApiType Decimal() => new DataApiType("decimal");
    /// <summary>Creates a double column type.</summary>
    public static DataApiType Double() => new DataApiType("double");
    /// <summary>Creates a duration column type.</summary>
    public static DataApiType Duration() => new DataApiType("duration");
    /// <summary>Creates a float column type.</summary>
    public static DataApiType Float() => new DataApiType("float");
    /// <summary>Creates an inet (IP address) column type.</summary>
    public static DataApiType Inet() => new DataApiType("inet");
    /// <summary>Creates an int column type.</summary>
    public static DataApiType Int() => new DataApiType("int");

    /// <summary>Creates a text column type.</summary>
    public static DataApiType Text() => new DataApiType("text");
    /// <summary>Creates a time column type.</summary>
    public static DataApiType Time() => new DataApiType("time");
    /// <summary>Creates a timestamp column type.</summary>
    public static DataApiType Timestamp() => new DataApiType("timestamp");
    /// <summary>Creates a uuid column type.</summary>
    public static DataApiType Uuid() => new DataApiType("uuid");

    /// <summary>Creates a list column type with the specified element type.</summary>
    public static DataApiType List(DataApiType valueType) => new ListDataApiType(valueType);
    /// <summary>Creates a map column type with string keys and the specified value type.</summary>
    public static DataApiType Map(DataApiType valueType) => new MapDataApiType(valueType);
    /// <summary>Creates a map column type with the specified key and value types.</summary>
    public static DataApiType Map(DataApiType keyType, DataApiType valueType) => new MapDataApiType(keyType, valueType);
    /// <summary>Creates a set column type with the specified element type.</summary>
    public static DataApiType Set(DataApiType valueType) => new ListDataApiType("set", valueType);
    /// <summary>Creates a vector column type with the specified dimension.</summary>
    public static DataApiType Vector(int dimension) => new VectorDataApiType(dimension);
    /// <summary>Creates a vectorize column type backed by the specified vectorization service.</summary>
    public static DataApiType Vectorize(VectorServiceOptions serviceOptions) => new VectorizeDataApiType(serviceOptions);
    /// <summary>Creates a vectorize column type with explicit dimensions, backed by the specified vectorization service.</summary>
    public static DataApiType Vectorize(int dimensions, VectorServiceOptions serviceOptions) => new VectorizeDataApiType(dimensions, serviceOptions);
    /// <summary>Creates a user-defined (UDT) column type with the specified type name.</summary>
    public static DataApiType UserDefined(string name) => new UserDefinedDataApiType(name);
}

/// <summary>
/// Vector data type
/// </summary>
public class VectorDataApiType : DataApiType
{
    /// <summary>
    /// The dimension of the vector
    /// </summary>
    [JsonPropertyName("dimension")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Dimension { get; set; }

    /// <summary>
    /// Vector data type
    /// </summary>
    /// <param name="dimension"></param>
    public VectorDataApiType(int? dimension) : base("vector")
    {
        Dimension = dimension;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

    internal override bool IsSimpleType => false;
}

/// <summary>
/// Vectorize data type
/// </summary>
public class VectorizeDataApiType : VectorDataApiType
{
    /// <summary>
    /// The vectorization service options
    /// </summary>
    [JsonPropertyName("service")]
    public VectorServiceOptions ServiceOptions { get; set; }

    /// <summary>
    /// Construct a Vectorize data type
    /// </summary>
    /// <param name="serviceOptions"></param>
    public VectorizeDataApiType(VectorServiceOptions serviceOptions) : base(null)
    {
        ServiceOptions = serviceOptions;
    }

    /// <summary>
    /// Construct a Vectorize data type with dimensions
    /// </summary>
    /// <param name="dimensions"></param>
    /// <param name="serviceOptions"></param>
    public VectorizeDataApiType(int? dimensions, VectorServiceOptions serviceOptions) : base(dimensions)
    {
        ServiceOptions = serviceOptions;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

}

/// <summary>
/// Represents a user-defined type (UDT) column type in the Data API.
/// </summary>
public class UserDefinedDataApiType : DataApiType
{
    /// <summary>
    /// The name of the user-defined type as registered in the Data API schema.
    /// </summary>
    [JsonPropertyName("udtName")]
    public string UserDefinedTypeName { get; set; }

    /// <summary>
    /// Initializes a new <see cref="UserDefinedDataApiType"/> with the given UDT name.
    /// </summary>
    public UserDefinedDataApiType(string name) : base("userDefined")
    {
        UserDefinedTypeName = name;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

    internal override bool IsSimpleType => false;
}

/// <summary>
/// Represents a list column type in the Data API, parameterized by an element type.
/// </summary>
public class ListDataApiType : DataApiType
{
    [JsonInclude]
    [JsonPropertyName("valueType")]
    internal object ValueTypeObject => ValueType.AsValueType;

    /// <summary>
    /// The Data API type of the list's elements.
    /// </summary>
    [JsonIgnore]
    public DataApiType ValueType { get; set; }

    /// <summary>
    /// Initializes a new list column type with the specified element type.
    /// </summary>
    public ListDataApiType(DataApiType valueType) : base("list")
    {
        ValueType = valueType;
    }

    /// <summary>
    /// Initializes a new list-like column type (e.g., set) with the given base type key and element type.
    /// </summary>
    public ListDataApiType(string baseType, DataApiType valueType) : base(baseType)
    {
        ValueType = valueType;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

    internal override bool IsSimpleType => false;

}

/// <summary>
/// Definition for a Map data type
/// </summary>
public class MapDataApiType : ListDataApiType
{
    [JsonInclude]
    [JsonPropertyName("keyType")]
    internal object KeyType { get; set; } = "text";

    /// <summary>
    /// Construct a Map data type with string keys and the specified value type
    /// </summary>
    /// <param name="valueType"></param>
    public MapDataApiType(DataApiType valueType) : base("map", valueType)
    {
    }

    /// <summary>
    /// Construct a Map data type with the specified key and value types
    /// </summary>
    /// <param name="keyType"></param>
    /// <param name="valueType"></param>
    public MapDataApiType(DataApiType keyType, DataApiType valueType) : base("map", valueType)
    {
        KeyType = keyType.AsValueType;
    }
}
