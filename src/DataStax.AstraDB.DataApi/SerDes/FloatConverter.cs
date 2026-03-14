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

using System;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Converter for float values that handles special string representations:
/// "NaN" -> float.NaN
/// "Infinity" -> float.PositiveInfinity
/// "-Infinity" -> float.NegativeInfinity
/// </summary>
public class FloatConverter : JsonConverter<float>
{
    public override float Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String)
        {
            return reader.GetString() switch
            {
                "NaN" => float.NaN,
                "Infinity" => float.PositiveInfinity,
                "-Infinity" => float.NegativeInfinity,
                _ => throw new JsonException($"Unexpected string value '{reader.GetString()}' for float type") // should never actually happen
            };
        }
        
        if (reader.TokenType == JsonTokenType.Number)
        {
            return reader.GetSingle();
        }
        
        throw new JsonException($"Unexpected token type '{reader.TokenType}' for float type");
    }

    public override void Write(Utf8JsonWriter writer, float value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case float.NaN:
                writer.WriteStringValue("NaN");
                break;
            case float.PositiveInfinity:
                writer.WriteStringValue("Infinity");
                break;
            case float.NegativeInfinity:
                writer.WriteStringValue("-Infinity");
                break;
            default:
                writer.WriteNumberValue(value);
                break;
        }
    }
}
