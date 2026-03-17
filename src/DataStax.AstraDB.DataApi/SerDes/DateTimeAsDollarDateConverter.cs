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
public class DateTimeAsDollarDateConverter<T> : JsonConverter<T>
{
    private static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Reads and converts a JSON <c>$date</c> object or Unix timestamp (milliseconds) to a <typeparamref name="T"/> value.
    /// </summary>
    public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return default;

        long unixTimeMilliseconds;

        if (reader.TokenType == JsonTokenType.StartObject)
        {
            if (reader.Read() && reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "$date")
            {
                reader.Read();
                if (reader.TokenType != JsonTokenType.Number)
                {
                    throw new JsonException("Expected number for Unix timestamp");
                }

                unixTimeMilliseconds = reader.GetInt64();
                reader.Read();
                if (reader.TokenType != JsonTokenType.EndObject)
                {
                    throw new JsonException("Expected end of object");
                }
            }
            else
            {
                throw new JsonException("Expected '$date' property.");
            }
        }
        else if (reader.TokenType == JsonTokenType.Number)
        {
            unixTimeMilliseconds = reader.GetInt64();
        }
        else
        {
            throw new JsonException($"Unexpected token {reader.TokenType} when reading date value.");
        }

        DateTimeOffset dto = UnixEpoch.AddMilliseconds(unixTimeMilliseconds);

        var underlyingType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        if (underlyingType == typeof(DateTimeOffset))
        {
            return (T)(object)dto;
        }
        else if (underlyingType == typeof(DateTime))
        {
            return (T)(object)dto.UtcDateTime;
        }
        else
        {
            throw new JsonException($"Cannot convert Unix timestamp to {typeof(T)}");
        }
    }

    /// <summary>
    /// Writes a <typeparamref name="T"/> value as a JSON <c>$date</c> object with Unix milliseconds.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNullValue();
            return;
        }

        long timestampMilliseconds;
        var underlyingType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        if (underlyingType == typeof(DateTime))
        {
            var dt = (DateTime)(object)value;
            if (dt.Kind == DateTimeKind.Local)
                throw new JsonException("DateTime with DateTimeKind.Local cannot be serialized. Convert to UTC using .ToUniversalTime() or use DateTimeOffset.");
            var dtUtc = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            timestampMilliseconds = (long)(dtUtc - UnixEpoch.UtcDateTime).TotalMilliseconds;
        }
        else if (underlyingType == typeof(DateTimeOffset))
        {
            var dto = (DateTimeOffset)(object)value;
            timestampMilliseconds = (long)(dto - UnixEpoch).TotalMilliseconds;
        }
        else
        {
            throw new JsonException($"Unsupported type: {typeof(T)}");
        }

        writer.WriteStartObject();
        writer.WriteNumber("$date", timestampMilliseconds);
        writer.WriteEndObject();
    }
}