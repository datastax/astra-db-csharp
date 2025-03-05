/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DataStax.AstraDB.DataApi.SerDes;

using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;


public class DocumentConverter<T> : JsonConverter<T>
{
    private static readonly Dictionary<PropertyInfo, string> FieldMappings;
    private static readonly Dictionary<string, PropertyInfo> ReverseMappings;

    static DocumentConverter()
    {
        FieldMappings = new Dictionary<PropertyInfo, string>();
        ReverseMappings = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);

        foreach (var prop in typeof(T).GetProperties())
        {
            var attr = prop.GetCustomAttribute<DocumentMappingAttribute>();
            if (attr != null)
            {
                string jsonName = attr.Field switch
                {
                    DocumentMappingField.Vectorize => "$vectorize",
                    DocumentMappingField.Vector => "$vector",
                    DocumentMappingField.Id => "_id",
                    DocumentMappingField.Similarity => "$similarity",
                    _ => prop.Name
                };
                FieldMappings[prop] = jsonName;
                ReverseMappings[jsonName] = prop;
            }
        }
    }

    public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject");

        T instance = Activator.CreateInstance<T>();
        PropertyInfo[] properties = typeof(T).GetProperties();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return instance;

            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            string propertyName = reader.GetString();
            reader.Read(); // Move to value

            PropertyInfo targetProp = ReverseMappings.TryGetValue(propertyName, out var mappedProp)
                ? mappedProp
                : properties.FirstOrDefault(p =>
                    p.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase) &&
                    !FieldMappings.ContainsKey(p));

            if (targetProp != null && targetProp.CanWrite)
            {
                object value = JsonSerializer.Deserialize(ref reader, targetProp.PropertyType, options);
                targetProp.SetValue(instance, value);
            }
            else
            {
                reader.Skip();
            }
        }

        throw new JsonException("Unexpected end of JSON");
    }

    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        foreach (PropertyInfo prop in typeof(T).GetProperties())
        {
            string propertyName = FieldMappings.TryGetValue(prop, out string mappedName)
                ? mappedName
                : options.PropertyNamingPolicy?.ConvertName(prop.Name) ?? prop.Name;

            object propValue = prop.GetValue(value);
            writer.WritePropertyName(propertyName);
            if (propertyName == "_id" && propValue.GetType() == typeof(ObjectId))
            {
                JsonSerializer.Serialize(writer, propValue.ToString(), prop.PropertyType, options);
            }
            else
            {
                JsonSerializer.Serialize(writer, propValue, prop.PropertyType, options);
            }
        }

        writer.WriteEndObject();
    }
    // private readonly Dictionary<Type, string> _attributeMappings = new Dictionary<Type, string>
    // {
    //     { typeof(VectorizeAttribute), "$vectorize" },
    //     { typeof(VectorAttribute), "$vector" },
    //     { typeof(IdAttribute), "_id" },
    //     { typeof(SimilarityAttribute), "$similarity" },
    // };

    // public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    // {
    //     if (reader.TokenType != JsonTokenType.StartObject)
    //     {
    //         throw new JsonException($"Cannot deserialize {typeToConvert.Name} from {reader.TokenType}.");
    //     }

    //     var obj = Activator.CreateInstance(typeToConvert);

    //     reader.Read();

    //     while (reader.TokenType != JsonTokenType.EndObject)
    //     {
    //         if (reader.TokenType == JsonTokenType.PropertyName)
    //         {
    //             string propertyName = reader.GetString();
    //             reader.Read();

    //             PropertyInfo property = typeToConvert.GetProperties().FirstOrDefault(p =>
    //             {
    //                 var attributes = p.GetCustomAttributes();
    //                 foreach (var attribute in attributes)
    //                 {
    //                     if (_attributeMappings.TryGetValue(attribute.GetType(), out string mappedName) && mappedName == propertyName)
    //                     {
    //                         return true;
    //                     }
    //                 }
    //                 if (p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name == propertyName || p.Name == propertyName)
    //                 {
    //                     return true;
    //                 }

    //                 return false;
    //             });

    //             if (property != null)
    //             {
    //                 object value = JsonSerializer.Deserialize(ref reader, property.PropertyType, options);
    //                 property.SetValue(obj, value);
    //             }
    //             else
    //             {
    //                 reader.Skip();
    //             }
    //         }

    //         reader.Read();
    //     }

    //     return (T)obj;
    // }

    // public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
    // {
    //     if (value == null)
    //     {
    //         writer.WriteNullValue();
    //         return;
    //     }

    //     Type type = value.GetType();
    //     PropertyInfo[] properties = type.GetProperties();

    //     writer.WriteStartObject();

    //     foreach (PropertyInfo property in properties)
    //     {
    //         string propertyName = property.Name;
    //         var attributes = property.GetCustomAttributes();
    //         foreach (var attribute in attributes)
    //         {
    //             if (attribute is JsonPropertyNameAttribute jsonNameAttr)
    //             {
    //                 propertyName = jsonNameAttr.Name;
    //             }
    //             else if (_attributeMappings.TryGetValue(attribute.GetType(), out string mappedName))
    //             {
    //                 propertyName = mappedName;
    //                 break;
    //             }
    //         }
    //         writer.WritePropertyName(propertyName);
    //         JsonSerializer.Serialize(writer, property.GetValue(value), property.PropertyType, options);
    //     }

    //     writer.WriteEndObject();
    // }
}