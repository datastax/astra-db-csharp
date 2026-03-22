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
using System.Text.Json;
using System.Text.Json.Serialization;

internal class SimpleDictionaryConverter
    : JsonConverter<Dictionary<string, object>>
{
    public override Dictionary<string, object> Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject");

        var dict = new Dictionary<string, object?>();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return dict;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected PropertyName");

            string propertyName = reader.GetString()!;
            reader.Read();

            dict[propertyName] = ReadValue(ref reader, options);
        }

        throw new JsonException("Incomplete JSON object");
    }

    private static object? ReadValue(
        ref Utf8JsonReader reader,
        JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.String => reader.GetString(),

            JsonTokenType.Number => reader.TryGetInt64(out var l)
                ? l
                : reader.GetDouble(),

            JsonTokenType.True => true,
            JsonTokenType.False => false,
            JsonTokenType.Null => null,

            JsonTokenType.StartObject =>
                JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    ref reader, options),

            JsonTokenType.StartArray =>
                JsonSerializer.Deserialize<List<object?>>(
                    ref reader, options),

            _ => throw new JsonException(
                $"Unsupported token: {reader.TokenType}")
        };
    }

    public override void Write(
        Utf8JsonWriter writer,
        Dictionary<string, object?> value,
        JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        foreach (var kvp in value)
        {
            writer.WritePropertyName(kvp.Key);
            WriteValue(writer, kvp.Value, options);
        }

        writer.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value, JsonSerializerOptions options)
    {
        if (value != null)
        {
            var type = value.GetType();
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
            {
                var keyType = type.GetGenericArguments()[0];
                if (keyType != typeof(string))
                {
                    var valueType = type.GetGenericArguments()[1];
                    writer.WriteStartArray();
                    var dict = (System.Collections.IDictionary)value;
                    foreach (var key in dict.Keys)
                    {
                        writer.WriteStartArray();
                        JsonSerializer.Serialize(writer, key, keyType, options);
                        JsonSerializer.Serialize(writer, dict[key], valueType, options);
                        writer.WriteEndArray();
                    }
                    writer.WriteEndArray();
                    return;
                }
            }
        }
        JsonSerializer.Serialize(writer, value, options);
    }
}