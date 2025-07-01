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

using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.SerDes;

public class FilterConverter<T> : JsonConverter<Filter<T>>
{
    public override Filter<T> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        // Deserialization logic (if needed)
        throw new NotImplementedException("Deserialization not implemented");
    }

    public override void Write(Utf8JsonWriter writer, Filter<T> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WritePropertyName(value.Name);
        JsonSerializer.Serialize(writer, value.Value, value.Value?.GetType() ?? typeof(object), options);
        writer.WriteEndObject();
    }
}

public class FilterConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert)
    {
        // Check if the type is Filter<T> (or a derived type)
        return typeToConvert.IsGenericType && typeToConvert.GetGenericTypeDefinition() == typeof(Filter<>);
    }

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        // Get the generic type T from Filter<T>
        Type genericType = typeToConvert.GetGenericArguments()[0];

        // Create an instance of FilterConverter<T> for the specific T
        Type converterType = typeof(FilterConverter<>).MakeGenericType(genericType);
        return (JsonConverter)Activator.CreateInstance(converterType);
    }
}