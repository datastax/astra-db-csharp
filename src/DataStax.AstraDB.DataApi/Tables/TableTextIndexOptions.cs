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

using DataStax.AstraDB.DataApi.Utils;
using DataStax.AstraDB.DataApi.SerDes;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Options for creating a text index on a table column
/// </summary>
public class TableTextIndexOptions
{

    /// <summary>
    /// The analyzer configuration for the text index.
    /// It can be a string, a TextAnalyzer value, an object, or an AnalyzerOptions instance.
    /// See https://docs.datastax.com/en/astra-db-serverless/databases/analyzers.html#supported-built-in-analyzers
    /// </summary>
    [JsonPropertyName("analyzer")]
    [JsonConverter(typeof(ObjectOrStringConverter))]
    public object Analyzer { get; set; }

}
