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

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Converter factory for serializing dictionaries in table operations.
/// Non-empty dictionaries are serialized as [[k1,v1],[k2,v2],...] format.
/// Empty dictionaries are serialized as {}.
/// </summary>
internal class TableDictionaryConverter : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert)
    {
        if (!typeToConvert.IsGenericType)
            return false;
            
        return typeToConvert.GetGenericTypeDefinition() == typeof(Dictionary<,>);
    }

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        Type keyType = typeToConvert.GetGenericArguments()[0];
        Type valueType = typeToConvert.GetGenericArguments()[1];

        JsonConverter converter = (JsonConverter)Activator.CreateInstance(
            typeof(TableDictionaryConverterInner<,>).MakeGenericType(keyType, valueType),
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            args: null,
            culture: null)!;

        return converter;
    }

    private class TableDictionaryConverterInner<TKey, TValue> : JsonConverter<Dictionary<TKey, TValue>>
    {
        public override Dictionary<TKey, TValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            // For table operations, we primarily need write support
            // Read support can be added if needed for deserialization
            throw new NotImplementedException("TableDictionaryConverter is designed for serialization only");
        }

        public override void Write(Utf8JsonWriter writer, Dictionary<TKey, TValue> value, JsonSerializerOptions options)
        {
            if (value.Count == 0)
            {
                // Empty dictionaries remain as {}
                writer.WriteStartObject();
                writer.WriteEndObject();
            }
            else
            {
                // Non-empty dictionaries: [[k1,v1],[k2,v2],...]
                writer.WriteStartArray();
                foreach (var kvp in value)
                {
                    writer.WriteStartArray();
                    JsonSerializer.Serialize(writer, kvp.Key, options);
                    JsonSerializer.Serialize(writer, kvp.Value, options);
                    writer.WriteEndArray();
                }
                writer.WriteEndArray();
            }
        }
    }
}
