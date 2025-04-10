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
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// A custom converter to handle serialization and deserialization of Guid values
/// </summary>
public class GuidConverter : JsonConverter<Guid>
{
    public override Guid Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.StartObject)
        {
            if (reader.Read() && reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "$uuid")
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String)
                {
                    string uuidString = reader.GetString();
                    if (Guid.TryParse(uuidString, out Guid uuidValue))
                    {
                        // Read the EndObject to fully consume the object
                        if (!reader.Read() || reader.TokenType != JsonTokenType.EndObject)
                            throw new JsonException("Expected end of $uuid object.");
                        return uuidValue;
                    }
                    else
                    {
                        throw new JsonException($"Invalid UUID format: {uuidString}");
                    }
                }
                else
                {
                    throw new JsonException("Expected string value for $uuid property.");
                }
            }
            else
            {
                throw new JsonException("Expected '$uuid' property.");
            }
        }
        else if (reader.TokenType == JsonTokenType.String)
        {
            string uuidString = reader.GetString();
            if (Guid.TryParse(uuidString, out Guid uuidValue))
            {
                return uuidValue;
            }
            else
            {
                throw new JsonException($"Invalid UUID format: {uuidString}");
            }
        }
        else
        {
            throw new JsonException($"Unsupported token type {reader.TokenType} for Guid value");
        }
    }

    public override void Write(Utf8JsonWriter writer, Guid value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("$uuid", value.ToString());
        writer.WriteEndObject();
    }
}