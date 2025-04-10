
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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Collections;

/// <summary>
/// A base document represented by a dictionary of string keys and object values.
/// </summary>
[JsonConverter(typeof(DocumentDictionaryConverter))]
public class Document : Dictionary<string, object>
{

}

/// <summary>
/// A custom JSON converter for the <see cref="Document"/> class.
/// </summary>
public class DocumentDictionaryConverter : JsonConverter<Document>
{
    public override Document Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected object");

        var doc = new Document();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            string key = reader.GetString();
            reader.Read();
            doc[key] = ParseValue(ref reader, options);
        }
        return doc;
    }

    private object ParseValue(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.Null:
                return null;
            case JsonTokenType.String:
                return reader.GetString();
            case JsonTokenType.Number:
                if (reader.TryGetInt32(out int intValue))
                    return intValue;
                if (reader.TryGetDouble(out double doubleValue))
                    return doubleValue;
                break;
            case JsonTokenType.True:
                return true;
            case JsonTokenType.False:
                return false;
            case JsonTokenType.StartArray:
                var list = new List<object>();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    list.Add(ParseValue(ref reader, options));
                }
                return list.ToArray(); // or list (List<object>) if preferred
            case JsonTokenType.StartObject:
                return JsonSerializer.Deserialize<Dictionary<string, object>>(ref reader, options);
            default:
                throw new JsonException($"Unsupported token: {reader.TokenType}");
        }
        throw new JsonException("Failed to parse value.");
    }

    public override void Write(Utf8JsonWriter writer, Document value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, (Dictionary<string, object>)value, options);
    }
}

