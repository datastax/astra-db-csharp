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
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

/// <summary>
/// JSON converter for <see cref="IPAddress"/>, serializing and deserializing IP addresses
/// as their standard string representation (e.g., "192.168.1.1" or "::1").
/// </summary>
public class IpAddressConverter : JsonConverter<IPAddress>
{
    /// <summary>
    /// Reads and converts a JSON string to an <see cref="IPAddress"/> value.
    /// </summary>
    public override IPAddress Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var token = reader.GetString();

        if (reader.TokenType != JsonTokenType.String || string.IsNullOrEmpty(token))
        {
            return null;
        }

        return IPAddress.Parse(token);
    }

    /// <summary>
    /// Writes an <see cref="IPAddress"/> value as a JSON string.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, IPAddress value, JsonSerializerOptions options)
    {
        var ipAddressString = string.Empty;

        if (value != null)
        {
            ipAddressString = value.ToString();
        }

        writer.WriteStringValue(ipAddressString);
    }
}