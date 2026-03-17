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
/// JSON converter that stores a value of type <typeparamref name="T"/> as a raw JSON string
/// (i.e., the object is serialized to JSON and that JSON is stored as a string value, and vice versa).
/// </summary>
public class JsonStringConverter<T> : JsonConverter<T>
{
    /// <summary>
    /// Reads and converts a JSON string to a <typeparamref name="T"/> instance by deserializing the string's contents.
    /// </summary>
    public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var jsonString = reader.GetString();
        if (string.IsNullOrEmpty(jsonString))
        {
            return default;
        }
        return JsonSerializer.Deserialize<T>(jsonString, options) ?? default;
    }

    /// <summary>
    /// Writes a <typeparamref name="T"/> value as a JSON string by serializing it to a JSON string value.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteStringValue(string.Empty);
            return;
        }
        var jsonString = JsonSerializer.Serialize(value, options);
        writer.WriteStringValue(jsonString);
    }
}