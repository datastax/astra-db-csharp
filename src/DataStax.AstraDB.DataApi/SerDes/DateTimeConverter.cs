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

/// <summary>
/// Handle serialization of DateTime when Kind is Unspecified
/// </summary>
public class DateTimeConverter : JsonConverter<DateTime>
{
    /// <summary>
    /// Default read handling
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="typeToConvert"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return reader.GetDateTime();
    }

    /// <summary>
    /// Set Kind to Local when Unspecified
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="value"></param>
    /// <param name="options"></param>
    public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
    {
        DateTime dateTimeToWrite = value;

        if (value.Kind == DateTimeKind.Unspecified)
        {
            dateTimeToWrite = DateTime.SpecifyKind(value, DateTimeKind.Local);
        }

        writer.WriteStringValue(dateTimeToWrite);
    }
}

/// <summary>
/// Handle serialization of DateTime? when Kind is Unspecified
/// </summary>
public class DateTimeNullableConverter : JsonConverter<DateTime?>
{
    /// <summary>
    /// Use default deserialization
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="typeToConvert"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public override DateTime? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return null;
        }

        return reader.GetDateTime();
    }

    /// <summary>
    /// If Kind is Unspecified, use Local
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="value"></param>
    /// <param name="options"></param>
    public override void Write(Utf8JsonWriter writer, DateTime? value, JsonSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNullValue();
            return;
        }

        DateTime dateTimeToWrite = value.Value;

        if (value.Value.Kind == DateTimeKind.Unspecified)
        {
            dateTimeToWrite = DateTime.SpecifyKind(value.Value, DateTimeKind.Local);
        }

        writer.WriteStringValue(dateTimeToWrite);
    }
}