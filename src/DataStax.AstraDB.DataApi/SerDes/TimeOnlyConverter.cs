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
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

#if NET6_0_OR_GREATER
/// <summary>
/// Handles TimeOnly deserialization from formats with sub-tick precision (>7 fractional second digits),
/// such as the nanosecond-precision strings returned by the Data API (e.g. "22:30:03.269601500").
/// System.Text.Json's built-in TimeOnly converter only supports up to 7 fractional digits.
/// </summary>
public class TimeOnlyConverter : JsonConverter<TimeOnly>
{
    private static readonly string[] Formats =
    {
        "HH:mm:ss",
        "HH:mm:ss.f",
        "HH:mm:ss.ff",
        "HH:mm:ss.fff",
        "HH:mm:ss.ffff",
        "HH:mm:ss.fffff",
        "HH:mm:ss.ffffff",
        "HH:mm:ss.fffffff",
    };

    /// <summary>
    /// Reads and converts a JSON strin from the Data API to a <see cref="TimeOnly"/> value.
    /// </summary>
    public override TimeOnly Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var s = reader.GetString();
        // Truncate fractional seconds to 7 digits (max supported by TimeOnly / .NET ticks)
        var dotIndex = s.IndexOf('.');
        if (dotIndex >= 0 && s.Length - dotIndex - 1 > 7)
        {
            s = s.Substring(0, dotIndex + 8); // keep dot + 7 digits
        }
        if (TimeOnly.TryParseExact(s, Formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
        {
            return result;
        }
        throw new JsonException($"Unable to convert \"{reader.GetString()}\" to TimeOnly.");
    }

    /// <summary>
    /// Writes a <see cref="TimeOnly"/> value as a Data API-compatible JSON string.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, TimeOnly value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture));
    }
}

/// <summary>
/// Nullable variant of <see cref="TimeOnlyConverter"/>.
/// </summary>
public class TimeOnlyNullableConverter : JsonConverter<TimeOnly?>
{
    private static readonly TimeOnlyConverter _inner = new();

    /// <summary>
    /// Reads and converts a JSON strin from the Data API to a <see cref="TimeOnly"/> value.
    /// </summary>
    public override TimeOnly? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;
        return _inner.Read(ref reader, typeof(TimeOnly), options);
    }

    /// <summary>
    /// Writes a <see cref="TimeOnly"/> value as a Data API-compatible JSON string.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, TimeOnly? value, JsonSerializerOptions options)
    {
        if (value == null)
            writer.WriteNullValue();
        else
            _inner.Write(writer, value.Value, options);
    }
}
#endif
