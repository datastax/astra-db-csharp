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
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;


public class TableIndexDefinition
{
    [JsonInclude]
    [JsonPropertyName("column")]
    internal string ColumnName { get; set; }

    [JsonInclude]
    [JsonPropertyName("options")]
    //[JsonConverter(typeof(StringBoolDictionaryConverter))]
    internal Dictionary<string, object> Options { get; set; } = new Dictionary<string, object>();

    [JsonIgnore]
    public bool CaseSensitive
    {
        get => Options.ContainsKey("caseSensitive") && bool.TryParse((string)Options["caseSensitive"], out var result) && result;
        set => Options["caseSensitive"] = value.ToString().ToLowerInvariant();
    }

    [JsonIgnore]
    public bool Normalize
    {
        get => Options.ContainsKey("normalize") && bool.TryParse((string)Options["normalize"], out var result) && result;
        set => Options["normalize"] = value.ToString().ToLowerInvariant();
    }

    [JsonIgnore]
    public bool Ascii
    {
        get => Options.ContainsKey("ascii") && bool.TryParse((string)Options["ascii"], out var result) && result;
        set => Options["ascii"] = value.ToString().ToLowerInvariant();
    }

    internal virtual string IndexCreationCommandName => "createIndex";
}

// public class TableIndexDefinition<TRow, TColumn> : TableIndexDefinition
// {
//     public Expression<Func<TRow, TColumn>> Column
//     {
//         set
//         {
//             ColumnName = value.GetMemberNameTree();
//         }
//     }
// }

// public class StringBoolDictionaryConverter : JsonConverter<Dictionary<string, string>>
// {
//     public override Dictionary<string, string> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
//     {
//         if (reader.TokenType != JsonTokenType.StartObject)
//         {
//             throw new JsonException("Expected StartObject token.");
//         }

//         var dictionary = new Dictionary<string, string>();

//         while (reader.Read())
//         {
//             if (reader.TokenType == JsonTokenType.EndObject)
//             {
//                 return dictionary;
//             }

//             if (reader.TokenType != JsonTokenType.PropertyName)
//             {
//                 throw new JsonException("Expected PropertyName token.");
//             }

//             string propertyName = reader.GetString();

//             reader.Read(); // Move to the value

//             string value;
//             switch (reader.TokenType)
//             {
//                 case JsonTokenType.True:
//                 case JsonTokenType.False:
//                     value = reader.GetBoolean().ToString().ToLowerInvariant();
//                     break;
//                 case JsonTokenType.String:
//                     value = reader.GetString();
//                     break;
//                 default:
//                     throw new JsonException($"Unexpected token type {reader.TokenType} for property {propertyName}.");
//             }

//             dictionary[propertyName] = value;
//         }

//         throw new JsonException("Unexpected end of JSON.");
//     }

//     public override void Write(Utf8JsonWriter writer, Dictionary<string, string> value, JsonSerializerOptions options)
//     {
//         writer.WriteStartObject();

//         foreach (var kvp in value)
//         {
//             writer.WritePropertyName(kvp.Key);
//             writer.WriteStringValue(kvp.Value);
//         }

//         writer.WriteEndObject();
//     }
// }
