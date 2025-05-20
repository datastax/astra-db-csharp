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

using DataStax.AstraDB.DataApi.Core.Commands;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

public class ByteArrayAsBinaryJsonConverter : JsonConverter<byte[]>
{
    public override byte[] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException($"Expected start of object when reading byte array, but got {reader.TokenType}.");
        }

        string base64String = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException($"Expected property name when reading byte array, but got {reader.TokenType}.");
            }

            string propertyName = reader.GetString();

            if (propertyName == DataApiKeywords.Binary)
            {
                reader.Read();
                if (reader.TokenType != JsonTokenType.String)
                {
                    throw new JsonException($"Expected string value for '{DataApiKeywords.Binary}', but got {reader.TokenType}.");
                }
                base64String = reader.GetString();
            }
            else
            {
                reader.Skip();
            }
        }

        if (base64String == null)
        {
            throw new JsonException($"Missing required property '{DataApiKeywords.Binary}'.");
        }

        try
        {
            return Convert.FromBase64String(base64String);
        }
        catch (FormatException ex)
        {
            throw new JsonException($"Invalid Base64 string: '{base64String}'.", ex);
        }
    }

    public override void Write(Utf8JsonWriter writer, byte[] value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString(DataApiKeywords.Binary, Convert.ToBase64String(value));
        writer.WriteEndObject();
    }
}