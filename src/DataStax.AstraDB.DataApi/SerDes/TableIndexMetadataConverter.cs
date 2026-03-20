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

/// <summary>
/// Custom converter for TableIndexMetadata that handles polymorphic deserialization
/// of the Definition property based on the indexType field.
/// </summary>
public class TableIndexMetadataConverter : JsonConverter<TableIndexMetadata>
{
    public override TableIndexMetadata Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using JsonDocument document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;

        var metadata = new TableIndexMetadata();

        // Read name
        if (root.TryGetProperty("name", out var nameElement))
        {
            metadata.Name = nameElement.GetString();
        }

        // Read indexType first
        if (root.TryGetProperty("indexType", out var indexTypeElement))
        {
            metadata.IndexType = indexTypeElement.GetString();
        }

        // Read definition with the appropriate type based on indexType
        if (root.TryGetProperty("definition", out var definitionElement))
        {
            Type definitionType = metadata.IndexType?.ToLowerInvariant() switch
            {
                "vector" => typeof(TableVectorIndexDefinition),
                "text" => typeof(TableTextIndexDefinition),
                "regular" => typeof(TableIndexDefinition),
                _ => typeof(TableIndexDefinition)
            };

            metadata.Definition = (TableBaseIndexDefinition)JsonSerializer.Deserialize(
                definitionElement.GetRawText(), 
                definitionType,
                options
            );
        }

        return metadata;
    }

    public override void Write(Utf8JsonWriter writer, TableIndexMetadata value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        writer.WritePropertyName("name");
        writer.WriteStringValue(value.Name);

        writer.WritePropertyName("definition");
        JsonSerializer.Serialize(writer, value.Definition, value.Definition?.GetType() ?? typeof(TableBaseIndexDefinition), options);

        writer.WritePropertyName("indexType");
        writer.WriteStringValue(value.IndexType);

        writer.WriteEndObject();
    }
}
