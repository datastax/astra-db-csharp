

using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Reflection;
using System.Text.Json.Serialization;

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
                        if (genericArguments.Length == 2 && genericArguments[0] == typeof(string))
                        {
                            return DataApiType.Map(GetDataApiTypeFromUnderlyingType(genericArguments[1]));
                        }
                        else
                        {
                            Console.WriteLine($"Warning: Unhandled Dictionary type. Only string keys are supported.");
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

public class DataApiType
{
    public DataApiType(string key)
    {
        Key = key;
    }

    [JsonPropertyName("type")]
    public string Key { get; set; }

    internal virtual object AsValueType => Key;

    internal virtual object AsColumnType => this;

    internal virtual bool IsSimpleType => true;

    public static DataApiType Ascii() => new DataApiType("ascii");
    public static DataApiType BigInt() => new DataApiType("bigint");
    public static DataApiType Blob() => new DataApiType("blob");
    public static DataApiType Boolean() => new DataApiType("boolean");
    public static DataApiType Date() => new DataApiType("date");
    public static DataApiType Decimal() => new DataApiType("decimal");
    public static DataApiType Double() => new DataApiType("double");
    public static DataApiType Duration() => new DataApiType("duration");
    public static DataApiType Float() => new DataApiType("float");
    public static DataApiType Inet() => new DataApiType("inet");
    public static DataApiType Int() => new DataApiType("int");

    public static DataApiType Text() => new DataApiType("text");
    public static DataApiType Time() => new DataApiType("time");
    public static DataApiType Timestamp() => new DataApiType("timestamp");
    public static DataApiType Uuid() => new DataApiType("uuid");

    public static DataApiType List(DataApiType valueType) => new ListDataApiType(valueType);
    public static DataApiType Map(DataApiType valueType) => new MapDataApiType(valueType);
    public static DataApiType Set(DataApiType valueType) => new ListDataApiType("set", valueType);
    public static DataApiType Vector(int dimension) => new VectorDataApiType(dimension);
    public static DataApiType Vectorize(int dimensions, VectorServiceOptions serviceOptions) => new VectorizeDataApiType(dimensions, serviceOptions);
    public static DataApiType UserDefined(string name) => new UserDefinedDataApiType(name);
}

public class VectorDataApiType : DataApiType
{
    /// <summary>
    /// The dimension of the vector
    /// </summary>
    [JsonPropertyName("dimension")]
    public int Dimension { get; set; }

    public VectorDataApiType(int dimension) : base("vector")
    {
        Dimension = dimension;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

    internal override bool IsSimpleType => false;
}

public class VectorizeDataApiType : VectorDataApiType
{
    /// <summary>
    /// The vectorization service options
    /// </summary>
    [JsonPropertyName("service")]
    public VectorServiceOptions ServiceOptions { get; set; }

    public VectorizeDataApiType(int dimensions, VectorServiceOptions serviceOptions) : base(dimensions)
    {
        ServiceOptions = serviceOptions;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

}

public class UserDefinedDataApiType : DataApiType
{
    [JsonPropertyName("udtName")]
    public string UserDefinedTypeName { get; set; }

    public UserDefinedDataApiType(string name) : base("userDefined")
    {
        UserDefinedTypeName = name;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

    internal override bool IsSimpleType => false;
}

public class ListDataApiType : DataApiType
{
    [JsonInclude]
    [JsonPropertyName("valueType")]
    internal object ValueTypeObject => ValueType.AsValueType;

    [JsonIgnore]
    public DataApiType ValueType { get; set; }

    public ListDataApiType(DataApiType valueType) : base("list")
    {
        ValueType = valueType;
    }

    public ListDataApiType(string baseType, DataApiType valueType) : base(baseType)
    {
        ValueType = valueType;
    }

    internal override object AsValueType => this;

    internal override object AsColumnType => this;

    internal override bool IsSimpleType => false;

}

public class MapDataApiType : ListDataApiType
{
    [JsonInclude]
    [JsonPropertyName("keyType")]
    internal object KeyType => "text";

    public MapDataApiType(DataApiType valueType) : base("map", valueType)
    {
    }
}
