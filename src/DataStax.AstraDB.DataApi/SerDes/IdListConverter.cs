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


public class IdListConverter : JsonConverter<List<object>>
{
    public override List<object> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
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

            ids.Add(ReadSingleValue(ref reader, typeToConvert, options));
        }

        throw new JsonException("Unexpected end of JSON while reading ID array");
    }

    private object ReadSingleValue(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        object value = reader.TokenType switch
        {
            JsonTokenType.Number => reader.TryGetInt32(out int intValue) ? intValue : reader.GetDouble(),
            JsonTokenType.String => ParseStringValue(reader.GetString()),
            JsonTokenType.True => true,
            JsonTokenType.False => false,
            JsonTokenType.Null => null,
            _ => JsonSerializer.Deserialize(ref reader, typeToConvert, options),
        };
        return value;
    }

    private object ParseStringValue(string value)
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
        JsonSerializer.Serialize(writer, value, value?.GetType() ?? typeof(object), options);
    }
}