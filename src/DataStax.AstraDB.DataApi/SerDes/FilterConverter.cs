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

using DataStax.AstraDB.DataApi.Core.Query;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

/// <summary>
/// JSON converter for <see cref="Filter{T}"/>, serializing each filter as a single-property
/// JSON object keyed by the filter's field name.
/// </summary>
public class FilterConverter<T> : JsonConverter<Filter<T>>
{
    /// <summary>
    /// Not implemented — filter deserialization is not supported.
    /// </summary>
    public override Filter<T> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        // Deserialization logic (if needed)
        throw new NotImplementedException("Deserialization not implemented");
    }

    /// <summary>
    /// Writes a <see cref="Filter{T}"/> as a JSON object keyed by the filter's field name.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, Filter<T> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WritePropertyName(value.Name);
        JsonSerializer.Serialize(writer, value.Value, value.Value?.GetType() ?? typeof(object), options);
        writer.WriteEndObject();
    }
}

/// <summary>
/// Factory that creates <see cref="FilterConverter{T}"/> instances for any closed
/// <c>Filter&lt;T&gt;</c> type encountered during serialization.
/// </summary>
public class FilterConverterFactory : JsonConverterFactory
{
    /// <summary>
    /// Returns <see langword="true"/> if this factory can convert the given type.
    /// </summary>
    public override bool CanConvert(Type typeToConvert)
    {
        // Check if the type is Filter<T> (or a derived type)
        return typeToConvert.IsGenericType && typeToConvert.GetGenericTypeDefinition() == typeof(Filter<>);
    }

    /// <summary>
    /// Creates a <see cref="FilterConverter{T}"/> for the specified <c>Filter&lt;T&gt;</c> type.
    /// </summary>
    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        // Get the generic type T from Filter<T>
        Type genericType = typeToConvert.GetGenericArguments()[0];

        // Create an instance of FilterConverter<T> for the specific T
        Type converterType = typeof(FilterConverter<>).MakeGenericType(genericType);
        return (JsonConverter)Activator.CreateInstance(converterType);
    }
}