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

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Configuration used to create an index on a table column
/// </summary>
public class TableIndexDefinition
{
    [JsonIgnore]
    internal string ColumnName { get; set; }

    private object _column;

    [JsonInclude]
    [JsonPropertyName("column")]
    internal virtual object Column
    {
        get
        {
            if (_column == null)
            {
                return ColumnName;
            }
            return _column;
        }
    }

    [JsonInclude]
    [JsonPropertyName("options")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal Dictionary<string, object> Options { get; set; } = new Dictionary<string, object>();

    /// <summary>
    /// Should the index be case sensitive?
    /// </summary>
    [JsonIgnore]
    public bool CaseSensitive
    {
        get => Options.ContainsKey("caseSensitive") && bool.TryParse((string)Options["caseSensitive"], out var result) && result;
        set => Options["caseSensitive"] = value.ToString().ToLowerInvariant();
    }

    /// <summary>
    /// Should the index normalize the text?
    /// </summary>
    [JsonIgnore]
    public bool Normalize
    {
        get => Options.ContainsKey("normalize") && bool.TryParse((string)Options["normalize"], out var result) && result;
        set => Options["normalize"] = value.ToString().ToLowerInvariant();
    }

    /// <summary>
    /// Should the index use ASCII conversion?
    /// </summary>
    [JsonIgnore]
    public bool Ascii
    {
        get => Options.ContainsKey("ascii") && bool.TryParse((string)Options["ascii"], out var result) && result;
        set => Options["ascii"] = value.ToString().ToLowerInvariant();
    }

    internal virtual string IndexCreationCommandName => "createIndex";
}
