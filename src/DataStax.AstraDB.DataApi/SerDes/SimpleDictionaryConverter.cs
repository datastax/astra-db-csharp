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
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

internal class SimpleDictionaryConverter : JsonConverter<object>
{
    public override bool CanConvert(Type typeToConvert)
    {
        return typeToConvert.IsGenericType && typeToConvert.GetGenericTypeDefinition() == typeof(Dictionary<,>);
    }
 
    public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected StartObject");
        }

        var dict = (IDictionary)Activator.CreateInstance(typeToConvert)!;
        var valueType = typeToConvert.GetGenericArguments()[1];
        
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return dict;
            
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected PropertyName");
            
            var propertyName = reader.GetString()!;
            reader.Read();
            dict[propertyName] = JsonSerializer.Deserialize(ref reader, valueType, options);
        }
        
        throw new JsonException("Incomplete JSON object");
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        if (value is not IDictionary dict)
        {
            throw new JsonException("Expected dictionary");
        }

        if (dict.Count == 0 || dict.GetType().GetGenericArguments()[0] == typeof(string))
        {
            writer.WriteStartObject();
            foreach (DictionaryEntry entry in dict)
            {
                writer.WritePropertyName(entry.Key.ToString()!);
                JsonSerializer.Serialize(writer, entry.Value, options);
            }
            writer.WriteEndObject();
        }
        else
        {
            var keyType = dict.GetType().GetGenericArguments()[0];
            var valueType = dict.GetType().GetGenericArguments()[1];
            
            writer.WriteStartArray();
            foreach (DictionaryEntry entry in dict)
            {
                writer.WriteStartArray();
                JsonSerializer.Serialize(writer, entry.Key, keyType, options);
                JsonSerializer.Serialize(writer, entry.Value, valueType, options);
                writer.WriteEndArray();
            }
            writer.WriteEndArray();
        }
    }
}
