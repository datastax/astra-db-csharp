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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using DataStax.AstraDB.DataApi.Tables;

namespace DataStax.AstraDB.DataApi.SerDes;

public class TableInsertManyResultConverter : JsonConverter<TableInsertManyResult>
{
    public override TableInsertManyResult Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject token");

        var result = new TableInsertManyResult();
        JsonDocument insertedIdsDoc = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                // Process insertedIds after all properties are read
                if (insertedIdsDoc != null && result.PrimaryKeys.Count > 0)
                {
                    result.InsertedIds = DeserializeInsertedIds(insertedIdsDoc, result.PrimaryKeys, options);
                    insertedIdsDoc.Dispose();
                }
                return result;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected PropertyName token");

            string propertyName = reader.GetString()!;
            reader.Read(); // Move to property value

            if (propertyName == "primaryKeySchema")
            {
                result.PrimaryKeys = JsonSerializer.Deserialize<Dictionary<string, PrimaryKeySchema>>(ref reader, options)
                    ?? throw new JsonException("Failed to deserialize primaryKeySchema");
            }
            else if (propertyName == "insertedIds")
            {
                // Capture raw JSON for insertedIds
                insertedIdsDoc = JsonDocument.ParseValue(ref reader);
            }
            else
            {
                // Skip unknown properties
                reader.Skip();
            }
        }

        throw new JsonException("Unexpected end of JSON");
    }

    private List<List<object>> DeserializeInsertedIds(JsonDocument insertedIdsDoc, Dictionary<string, PrimaryKeySchema> primaryKeys, JsonSerializerOptions options)
    {
        var insertedIds = new List<List<object>>();
        if (insertedIdsDoc.RootElement.ValueKind != JsonValueKind.Array)
            throw new JsonException("Expected array for insertedIds");

        foreach (var rowElement in insertedIdsDoc.RootElement.EnumerateArray())
        {
            if (rowElement.ValueKind != JsonValueKind.Array)
                throw new JsonException("Expected array for insertedIds row");

            var row = new List<object>();
            int index = 0;

            foreach (var valueElement in rowElement.EnumerateArray())
            {
                if (index >= primaryKeys.Count)
                    throw new JsonException("More values in row than schema fields");

                var fieldName = primaryKeys.Keys.ElementAt(index);
                var schema = primaryKeys[fieldName];
                object value = DeserializeValue(valueElement, schema, options);
                row.Add(value);
                index++;
            }

            if (index != primaryKeys.Count)
                throw new JsonException("Fewer values in row than schema fields");

            insertedIds.Add(row);
        }

        return insertedIds;
    }

    private object DeserializeValue(JsonElement element, PrimaryKeySchema schema, JsonSerializerOptions options)
    {
        try
        {
            switch (schema.Type)
            {
                case "text":
                    return element.GetString()!;
                case "vector":
                    var floatList = JsonSerializer.Deserialize<List<float>>(element, options)!;
                    return floatList.ToArray();
                case "int":
                    return element.GetInt32();
                case "bigint":
                    return element.GetInt64();
                case "decimal":
                    return element.GetDecimal();
                case "double":
                    return element.GetDouble();
                case "float":
                    return element.GetSingle();
                case "boolean":
                    return element.GetBoolean();
                case "timestamp":
                    return DateTime.Parse(element.GetString()!);
                case "uuid":
                    return Guid.Parse(element.GetString()!);
                default:
                    throw new JsonException($"Unsupported schema type: {schema.Type}");
            }
        }
        catch (Exception ex)
        {
            throw new JsonException($"Failed to deserialize value: {ex.Message}");
        }
    }

    public override void Write(Utf8JsonWriter writer, TableInsertManyResult value, JsonSerializerOptions options)
    {
        throw new NotSupportedException("Serialization is not implemented for TableInsertManyResult");
    }
}