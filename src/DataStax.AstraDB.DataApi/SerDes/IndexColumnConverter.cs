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

using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

/// <summary>
/// A custom converter to handle serdes of the index definition's "column" multiform attribute
/// (e.g. `"my_column"` vs. `{"my_map_column": "$values}`).
/// </summary>
public class IndexColumnConverter : JsonConverter<object>
{
    public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String)
        {
            return reader.GetString();
        }

        if (reader.TokenType == JsonTokenType.StartObject)
        {
            using JsonDocument document = JsonDocument.ParseValue(ref reader);
            return DeserializationUtils.UnwrapJsonElement(document.RootElement);
        }

        throw new JsonException($"Unexpected token type {reader.TokenType} for column property.");
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNullValue();
            return;
        }

        if (value is string stringValue)
        {
            writer.WriteStringValue(stringValue);
            return;
        }

        if (value is Dictionary<string, string> dictValue)
        {
            JsonSerializer.Serialize(writer, dictValue, options);
            return;
        }

        throw new JsonException($"Unexpected value type {value.GetType()} for column property.");
    }
}
