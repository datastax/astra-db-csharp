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

using DataStax.AstraDB.DataApi.Core.Commands;
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
    private static readonly List<string> PropertyNamesToIgnore = new();

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
                    DocumentMappingField.Vectorize => DataApiKeywords.Vectorize,
                    DocumentMappingField.Vector => DataApiKeywords.Vector,
                    DocumentMappingField.Id => DataApiKeywords.Id,
                    DocumentMappingField.Similarity => DataApiKeywords.Similarity,
                    _ => prop.Name
                };
                FieldMappings[prop] = jsonName;
                ReverseMappings[jsonName] = prop;
                PropertyNamesToIgnore.Add(prop.Name);
            }
            else
            {
                if (prop.Name == DataApiKeywords.Id)
                {
                    FieldMappings[prop] = DataApiKeywords.Id;
                    ReverseMappings[DataApiKeywords.Id] = prop;
                    PropertyNamesToIgnore.Add(prop.Name);
                }
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

        foreach (var prop in typeof(T).GetProperties())
        {
            if (FieldMappings.TryGetValue(prop, out string mappedName))
            {
                string propertyName = mappedName;
                object propValue = prop.GetValue(value);

                writer.WritePropertyName(propertyName);

                // if (propertyName == DataApiKeywords.Id && propValue?.GetType() == typeof(ObjectId))
                // {
                //     JsonSerializer.Serialize(writer, propValue.ToString(), typeof(string), options);
                // }
                // else
                // {
                JsonSerializer.Serialize(writer, propValue, prop.PropertyType, options);
                //}
            }
        }

        // Delegate to default serialization for non-special properties
        var defaultOptions = new JsonSerializerOptions
        {
            AllowTrailingCommas = options.AllowTrailingCommas,
            DefaultBufferSize = options.DefaultBufferSize,
            DictionaryKeyPolicy = options.DictionaryKeyPolicy,
            Encoder = options.Encoder,
            IgnoreReadOnlyFields = options.IgnoreReadOnlyFields,
            IgnoreReadOnlyProperties = options.IgnoreReadOnlyProperties,
            IncludeFields = options.IncludeFields,
            MaxDepth = options.MaxDepth,
            NumberHandling = options.NumberHandling,
            PropertyNameCaseInsensitive = options.PropertyNameCaseInsensitive,
            PropertyNamingPolicy = options.PropertyNamingPolicy,
            ReadCommentHandling = options.ReadCommentHandling,
            ReferenceHandler = options.ReferenceHandler,
            UnknownTypeHandling = options.UnknownTypeHandling,
            WriteIndented = options.WriteIndented
        };
        string defaultJson = JsonSerializer.Serialize(value, typeof(T), defaultOptions);
        using var doc = JsonDocument.Parse(defaultJson);
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            if (!PropertyNamesToIgnore.Contains(prop.Name))
            {
                writer.WritePropertyName(prop.Name);
                prop.Value.WriteTo(writer);
            }
        }

        writer.WriteEndObject();
    }
}