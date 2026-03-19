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

using DataStax.AstraDB.DataApi.SerDes;
using DataStax.AstraDB.DataApi.Utils;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Configuration used to create an index on a table column
/// </summary>
public abstract class TableBaseIndexDefinition
{
    [JsonIgnore]
    internal string ColumnName { get; set; }

    private object _column;

    [JsonPropertyName("column")]
    [JsonConverter(typeof(IndexColumnConverter))]
    public virtual object Column
    {
        get
        {
            if (_column == null)
            {
                return ColumnName;
            }
            return _column;
        }
        internal set => _column = value;

    }

    [JsonInclude]
    [JsonPropertyName("options")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonConverter(typeof(SimpleDictionaryConverter))]
    internal Dictionary<string, object> Options { get; set; }

    internal abstract string IndexCreationCommandName { get; }
}
