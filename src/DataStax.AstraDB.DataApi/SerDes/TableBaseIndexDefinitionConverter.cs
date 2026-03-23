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
/// Custom converter for polymorphic deserialization of TableBaseIndexDefinition
/// based on the indexType field from the parent TableIndexMetadata object.
/// </summary>
public class TableBaseIndexDefinitionConverter : JsonConverter<TableBaseIndexDefinition>
{
    private readonly string _indexType;

    public TableBaseIndexDefinitionConverter(string indexType)
    {
        _indexType = indexType;
    }

    public override TableBaseIndexDefinition Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        // Parse the JSON into a JsonDocument to inspect it
        using JsonDocument document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;

        // Determine the concrete type based on indexType
        Type concreteType = _indexType?.ToLowerInvariant() switch
        {
            "vector" => typeof(TableVectorIndexDefinition),
            "text" => typeof(TableTextIndexDefinition),
            "regular" => typeof(TableIndexDefinition),
            _ => typeof(TableIndexDefinition) // Default to regular index
        };

        // Deserialize to the appropriate concrete type
        return (TableBaseIndexDefinition)JsonSerializer.Deserialize(root.GetRawText(), concreteType, options);
    }

    public override void Write(Utf8JsonWriter writer, TableBaseIndexDefinition value, JsonSerializerOptions options)
    {
        // Serialize using the actual runtime type
        JsonSerializer.Serialize(writer, value, value.GetType(), options);
    }
}
