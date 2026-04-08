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
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Options for creating an index on a table column
/// </summary>
public class TableIndexOptions
{

    /// <summary>
    /// Should the index be case sensitive?
    /// </summary>
    [JsonPropertyName("caseSensitive")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool CaseSensitive { get; set; }

    /// <summary>
    /// Should the index normalize the text?
    /// </summary>
    [JsonPropertyName("normalize")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Normalize { get; set; }

    /// <summary>
    /// Should the index use ASCII conversion?
    /// </summary>
    [JsonPropertyName("ascii")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Ascii { get; set; }

}
