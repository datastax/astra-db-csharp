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

using DataStax.AstraDB.DataApi.Tables;
using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

public class RowConverter<T> : JsonConverter<T> where T : class
{
    public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject token");

        T instance = Activator.CreateInstance<T>();
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !p.GetCustomAttributes<ColumnIgnoreAttribute>().Any())
            .ToDictionary(p => GetPropertyName(p, true), p => p);

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return instance;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected PropertyName token");

            string propertyName = reader.GetString();
            if (properties.TryGetValue(propertyName, out PropertyInfo property))
            {
                reader.Read();

                if (property.GetCustomAttribute<ColumnVectorizeAttribute>() != null)
                {
                    if (reader.TokenType == JsonTokenType.StartArray)
                    {
                        var floatList = new System.Collections.Generic.List<float>();
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        {
                            if (reader.TokenType == JsonTokenType.Number)
                            {
                                floatList.Add(reader.GetSingle());
                            }
                            else
                            {
                                throw new JsonException($"Unexpected token type '{reader.TokenType}' within float array for property '{propertyName}'. Expected Number.");
                            }
                        }
                        property.SetValue(instance, floatList.ToArray());
                    }
                    else
                    {
                        throw new JsonException($"Expected StartArray token for ColumnVectorize property '{propertyName}'.");
                    }
                }
                else if (property.GetCustomAttribute<ColumnJsonStringAttribute>() != null)
                {
                    if (reader.TokenType == JsonTokenType.String)
                    {
                        string jsonString = reader.GetString();
                        object value = JsonSerializer.Deserialize(jsonString, property.PropertyType, options);
                        property.SetValue(instance, value);
                    }
                }
                else
                {
                    object value = JsonSerializer.Deserialize(ref reader, property.PropertyType, options);
                    property.SetValue(instance, value);
                }
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

        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !p.GetCustomAttributes<ColumnIgnoreAttribute>().Any());

        foreach (var property in properties)
        {
            string propertyName = GetPropertyName(property, false);
            object propertyValue = property.GetValue(value);

            if (propertyValue == null && options.DefaultIgnoreCondition == JsonIgnoreCondition.WhenWritingNull)
                continue;

            writer.WritePropertyName(propertyName);

            if (property.GetCustomAttribute<ColumnJsonStringAttribute>() != null)
            {
                string jsonString = JsonSerializer.Serialize(propertyValue, property.PropertyType, options);
                writer.WriteStringValue(jsonString);
            }
            else
            {
                JsonSerializer.Serialize(writer, propertyValue, property.PropertyType, options);
            }
        }

        writer.WriteEndObject();
    }

    private static string GetPropertyName(PropertyInfo property, bool forDeserialization)
    {
        var documentMappingAttribute = property.GetCustomAttribute<DocumentMappingAttribute>();
        if (documentMappingAttribute != null && forDeserialization)
        {
            if (documentMappingAttribute.Field == DocumentMappingField.Similarity)
            {
                return "$similarity";
            }

        }

        var nameAttribute = property.GetCustomAttribute<ColumnNameAttribute>();
        return nameAttribute == null ? property.Name : nameAttribute.ColumnName;
    }
}