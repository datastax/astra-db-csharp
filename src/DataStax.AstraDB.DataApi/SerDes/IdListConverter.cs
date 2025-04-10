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
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// A custom converter to handle serialization and deserialization of a list of IDs
/// </summary>
/// <remarks>
/// We recommend using strongly-typed IDs instead of objects.
/// </remarks>
public class IdListConverter : JsonConverter<List<object>>
{
    private static readonly GuidConverter _guidConverter = new();
    private static readonly ObjectIdConverter _objectIdConverter = new();
    private static readonly DateTimeConverter<DateTime> _dateTimeConverter = new();

    public override List<object> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return default;

        if (reader.TokenType != JsonTokenType.StartArray)
        {
            throw new JsonException("Expected StartArray for ID list");
        }

        var ids = new List<object>();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
            {
                return ids;
            }

            ids.Add(ReadSingleIdValue(ref reader, typeToConvert, options));
        }

        throw new JsonException("Unexpected end of JSON while reading ID array");
    }

    static internal object ReadSingleIdValue(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.Number:
                if (typeToConvert == typeof(int) || typeToConvert == typeof(int?))
                {
                    if (reader.TryGetInt32(out int intValue))
                        return intValue;
                    throw new JsonException($"Expected an integer value for type '{typeToConvert}', but got a non-integer number.");
                }
                else if (typeToConvert == typeof(double) || typeToConvert == typeof(double?))
                {
                    return reader.GetDouble();
                }
                else
                {
                    if (reader.TryGetInt32(out int intValue))
                        return intValue;
                    return reader.GetDouble();
                }

            case JsonTokenType.String:
                return ParseStringValue(reader.GetString());

            case JsonTokenType.True:
                return true;

            case JsonTokenType.False:
                return false;

            case JsonTokenType.Null:
                return null;

            case JsonTokenType.StartObject:
                Utf8JsonReader peekReader = reader;
                if (!peekReader.Read() || peekReader.TokenType != JsonTokenType.PropertyName)
                    throw new JsonException("Expected property name in ID object.");

                string propertyName = peekReader.GetString();
                return propertyName switch
                {
                    "$uuid" => _guidConverter.Read(ref reader, typeof(Guid), options),
                    "$objectId" => _objectIdConverter.Read(ref reader, typeof(ObjectId), options),
                    "$date" => _dateTimeConverter.Read(ref reader, typeof(DateTime), options),
                    _ => throw new JsonException($"Unsupported object type '{propertyName}' in ID list.")
                };

            default:
                throw new JsonException($"Unsupported token type {reader.TokenType} for ID value");
        }
    }

    static internal object ParseStringValue(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        if (Guid.TryParse(value, out Guid guidValue))
            return guidValue;

        if (ObjectId.TryParse(value, out ObjectId objectIdValue))
            return objectIdValue;

        if (DateTime.TryParse(value, null, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime dateTimeValue))
            return dateTimeValue;

        return value;
    }

    public override void Write(Utf8JsonWriter writer, List<object> value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        if (value != null)
        {
            foreach (var item in value)
            {
                JsonSerializer.Serialize(writer, item, options);
            }
        }
        writer.WriteEndArray();
    }
}