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
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// A custom converter to handle serialization and deserialization of ObjectId values
/// </summary>
public class ObjectIdConverter : JsonConverter<ObjectId>
{
    public override ObjectId Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.StartObject)
        {
            if (reader.Read() && reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "$objectId")
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String)
                {
                    string objectIdString = reader.GetString();
                    if (ObjectId.TryParse(objectIdString, out ObjectId objectIdValue))
                    {
                        if (!reader.Read() || reader.TokenType != JsonTokenType.EndObject)
                            throw new JsonException("Expected end of $objectId object.");
                        return objectIdValue;
                    }
                    throw new JsonException($"Invalid ObjectId format: {objectIdString}");
                }
                throw new JsonException("Expected string value for $objectId property.");
            }
            throw new JsonException("Expected '$objectId' property.");
        }
        else if (reader.TokenType == JsonTokenType.String)
        {
            string objectIdString = reader.GetString();
            if (ObjectId.TryParse(objectIdString, out ObjectId objectIdValue))
            {
                return objectIdValue;
            }
            throw new JsonException($"Invalid ObjectId format: {objectIdString}");
        }
        else
        {
            throw new JsonException($"Unsupported token type {reader.TokenType} for ObjectId value");
        }
    }

    public override void Write(Utf8JsonWriter writer, ObjectId value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("$objectId", value.ToString());
        writer.WriteEndObject();
    }
}