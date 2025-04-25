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

using DataStax.AstraDB.DataApi.Tables;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

public class ColumnConverter : JsonConverter<Column>
{
    public override bool CanConvert(Type typeToConvert) => typeof(Column).IsAssignableFrom(typeToConvert);

    public override Column Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected StartObject token");
        }

        using JsonDocument document = JsonDocument.ParseValue(ref reader);
        JsonElement root = document.RootElement;

        if (!root.TryGetProperty("type", out JsonElement typeElement) || typeElement.ValueKind != JsonValueKind.String)
        {
            throw new JsonException("Missing or invalid 'type' property in Column object.");
        }

        string typeValue = typeElement.GetString();

        string jsonText = root.GetRawText();

        if (typeValue == "vector")
        {
            if (root.TryGetProperty("service", out _))
            {
                return JsonSerializer.Deserialize<VectorizeColumn>(jsonText, options)
                    ?? throw new JsonException("Deserialization returned null for VectorGenerationColumn");
            }
            return JsonSerializer.Deserialize<VectorColumn>(jsonText, options);
        }

        return typeValue switch
        {
            "text" => JsonSerializer.Deserialize<TextColumn>(jsonText, options),
            "ascii" => JsonSerializer.Deserialize<TextColumn>(jsonText, options),
            "varchar" => JsonSerializer.Deserialize<TextColumn>(jsonText, options),
            "inet" => JsonSerializer.Deserialize<IPAddressColumn>(jsonText, options),
            "int" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
            "tinyint" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
            "smallint" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
            "varint" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
            "bigint" => JsonSerializer.Deserialize<LongColumn>(jsonText, options),
            "decimal" => JsonSerializer.Deserialize<DecimalColumn>(jsonText, options),
            "double" => JsonSerializer.Deserialize<DoubleColumn>(jsonText, options),
            "float" => JsonSerializer.Deserialize<FloatColumn>(jsonText, options),
            "map" => JsonSerializer.Deserialize<DictionaryColumn>(jsonText, options),
            "set" => JsonSerializer.Deserialize<SetColumn>(jsonText, options),
            "list" => JsonSerializer.Deserialize<ListColumn>(jsonText, options),
            "boolean" => JsonSerializer.Deserialize<BooleanColumn>(jsonText, options),
            "date" => JsonSerializer.Deserialize<DateColumn>(jsonText, options),
            "time" => JsonSerializer.Deserialize<DateColumn>(jsonText, options),
            "timestamp" => JsonSerializer.Deserialize<DateColumn>(jsonText, options),
            "vector" => JsonSerializer.Deserialize<VectorColumn>(jsonText, options),
            "uuid" => JsonSerializer.Deserialize<GuidColumn>(jsonText, options),
            "blob" => JsonSerializer.Deserialize<BlobColumn>(jsonText, options),
            _ => throw new JsonException($"Unknown Column type '{typeValue}' encountered.")
        };
    }

    public override void Write(Utf8JsonWriter writer, Column value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, value, value.GetType(), options);
    }
}
