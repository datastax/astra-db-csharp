// /*
//  * Copyright DataStax, Inc.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// using DataStax.AstraDB.DataApi.Core;
// using DataStax.AstraDB.DataApi.Tables;
// using System;
// using System.Text.Json;
// using System.Text.Json.Serialization;

// namespace DataStax.AstraDB.DataApi.SerDes;

// /// <summary>
// /// JsonConverter to handle Table Columns
// /// </summary>
// public class ColumnConverter : JsonConverter<Column>
// {
//     /// <summary>
//     /// Check applicability of this converter
//     /// </summary>
//     /// <param name="typeToConvert"></param>
//     /// <returns></returns>
//     public override bool CanConvert(Type typeToConvert) => typeof(Column).IsAssignableFrom(typeToConvert);

//     /// <summary>
//     /// Handle read.
//     /// </summary>
//     /// <param name="reader"></param>
//     /// <param name="typeToConvert"></param>
//     /// <param name="options"></param>
//     /// <returns></returns>
//     /// <exception cref="JsonException"></exception>
//     public override Column Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
//     {
//         if (reader.TokenType != JsonTokenType.StartObject)
//         {
//             throw new JsonException("Expected StartObject token");
//         }

//         using JsonDocument document = JsonDocument.ParseValue(ref reader);
//         JsonElement root = document.RootElement;

//         if (!root.TryGetProperty("type", out JsonElement typeElement) || typeElement.ValueKind != JsonValueKind.String)
//         {
//             throw new JsonException("Missing or invalid 'type' property in Column object.");
//         }

//         string typeValue = typeElement.GetString();

//         // Manually construct the appropriate Column-derived object to avoid calling
//         // JsonSerializer.Deserialize on types that would again use this converter.
//         if (string.Equals(typeValue, "vector", StringComparison.OrdinalIgnoreCase))
//         {
//             // vector types contain a "dimension" and optionally a "service"
//             int dimension = 0;
//             if (root.TryGetProperty("dimension", out JsonElement dimEl) && dimEl.ValueKind == JsonValueKind.Number)
//             {
//                 dimEl.TryGetInt32(out dimension);
//             }

//             if (root.TryGetProperty("service", out JsonElement serviceEl))
//             {
//                 var vecGen = new VectorizeColumn();
//                 // dimension
//                 vecGen.Dimension = dimension;

//                 // service options - use JsonSerializer for nested non-Column type
//                 if (serviceEl.ValueKind != JsonValueKind.Null)
//                 {
//                     vecGen.ServiceOptions = JsonSerializer.Deserialize<VectorServiceOptions>(serviceEl.GetRawText(), options);
//                 }

//                 // optional keyType/valueType
//                 if (root.TryGetProperty("keyType", out JsonElement kt) && kt.ValueKind == JsonValueKind.String)
//                 {
//                     vecGen.KeyType = (DataApiType)Enum.Parse(typeof(DataApiType), kt.GetString(), true);
//                 }
//                 if (root.TryGetProperty("valueType", out JsonElement vt) && vt.ValueKind == JsonValueKind.String)
//                 {
//                     vecGen.ValueType = (DataApiType)Enum.Parse(typeof(DataApiType), vt.GetString(), true);
//                 }

//                 return vecGen;
//             }

//             var vecCol = new VectorColumn();
//             vecCol.Dimension = dimension;
//             if (root.TryGetProperty("keyType", out JsonElement kt2) && kt2.ValueKind == JsonValueKind.String)
//             {
//                 vecCol.KeyType = (DataApiType)Enum.Parse(typeof(DataApiType), kt2.GetString(), true);
//             }
//             if (root.TryGetProperty("valueType", out JsonElement vt2) && vt2.ValueKind == JsonValueKind.String)
//             {
//                 vecCol.ValueType = (DataApiType)Enum.Parse(typeof(DataApiType), vt2.GetString(), true);
//             }

//             return vecCol;
//         }

//         // Default: plain Column
//         var col = new Column();
//         if (!string.IsNullOrEmpty(typeValue))
//         {
//             col.Type = (DataApiType)Enum.Parse(typeof(DataApiType), typeValue, true);
//         }

//         if (root.TryGetProperty("keyType", out JsonElement keyEl) && keyEl.ValueKind == JsonValueKind.String)
//         {
//             col.KeyType = (DataApiType)Enum.Parse(typeof(DataApiType), keyEl.GetString(), true);
//         }

//         if (root.TryGetProperty("valueType", out JsonElement valEl) && valEl.ValueKind == JsonValueKind.String)
//         {
//             col.ValueType = (DataApiType)Enum.Parse(typeof(DataApiType), valEl.GetString(), true);
//         }

//         return col;
//     }

//     /// <summary>
//     /// Handle write.
//     /// </summary>
//     /// <param name="writer"></param>
//     /// <param name="value"></param>
//     /// <param name="options"></param>
//     public override void Write(Utf8JsonWriter writer, Column value, JsonSerializerOptions options)
//     {
//         if (value == null)
//         {
//             writer.WriteNullValue();
//             return;
//         }

//         writer.WriteStartObject();

//         if (value.Type != DataApiType.None)
//         {
//             writer.WriteString("type", value.Type.ToString().ToLowerInvariant());
//         }

//         if (value.KeyType != DataApiType.None)
//         {
//             writer.WriteString("keyType", value.KeyType.ToString().ToLowerInvariant());
//         }

//         if (value.ValueType != DataApiType.None)
//         {
//             writer.WriteString("valueType", value.ValueType.ToString().ToLowerInvariant());
//         }

//         if (value is VectorColumn vec)
//         {
//             writer.WriteNumber("dimension", vec.Dimension);

//             if (vec is VectorizeColumn vgen && vgen.ServiceOptions != null)
//             {
//                 writer.WritePropertyName("service");
//                 JsonSerializer.Serialize(writer, vgen.ServiceOptions, options);
//             }
//         }

//         writer.WriteEndObject();
//     }
// }
