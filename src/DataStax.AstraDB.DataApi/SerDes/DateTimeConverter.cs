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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

/// <summary>
/// A custom converter to handle DataApi DateTime values
/// </summary>
/// <typeparam name="T"></typeparam>
public class DateTimeConverter<T> : JsonConverter<T>
{
    private static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return default;

        if (reader.TokenType == JsonTokenType.StartObject)
        {
            if (reader.Read() && reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "$date")
            {
                reader.Read();
                if (reader.TokenType != JsonTokenType.Number)
                {
                    throw new JsonException("Expected number for Unix timestamp");
                }

                long unixTimeMilliseconds = reader.GetInt64();
                reader.Read();
                if (reader.TokenType != JsonTokenType.EndObject)
                {
                    throw new JsonException("Expected end of object");
                }

                DateTimeOffset dto = UnixEpoch.AddMilliseconds(unixTimeMilliseconds);

                if (typeof(T) == typeof(DateTimeOffset))
                {
                    return (T)(object)dto;
                }
                else if (typeof(T) == typeof(DateTime))
                {
                    return (T)(object)dto.UtcDateTime;
                }
                else
                {
                    throw new JsonException($"Cannot convert Unix timestamp to {typeof(T)}");
                }
            }
            else
            {
                throw new JsonException("Expected '$date' property.");
            }
        }
        else if (reader.TokenType == JsonTokenType.Number)
        {
            long unixTimeMilliseconds = reader.GetInt64();
            DateTimeOffset dto = UnixEpoch.AddMilliseconds(unixTimeMilliseconds);

            if (typeof(T) == typeof(DateTimeOffset))
            {
                return (T)(object)dto;
            }
            else if (typeof(T) == typeof(DateTime))
            {
                return (T)(object)dto.UtcDateTime;
            }
            else
            {
                throw new JsonException($"Cannot convert Unix timestamp to {typeof(T)}");
            }
        }
        else
        {
            return default;
        }
    }

    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNullValue();
            return;
        }

        long timestampMilliseconds;
        switch (value)
        {
            case DateTime dt:
                timestampMilliseconds = (long)(dt.ToUniversalTime() - UnixEpoch).TotalMilliseconds;
                break;
            case DateTimeOffset dto:
                timestampMilliseconds = (long)(dto - UnixEpoch).TotalMilliseconds;
                break;
            default:
                throw new JsonException($"Unsupported type: {value.GetType()}");
        }

        writer.WriteStartObject();
        writer.WriteNumber("$date", timestampMilliseconds);
        writer.WriteEndObject();
    }
}