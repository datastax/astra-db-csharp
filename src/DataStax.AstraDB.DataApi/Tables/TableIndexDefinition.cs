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
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

public class TableIndexDefinition<TRow, TColumn> : TableIndexDefinition
{
    public Expression<Func<TRow, TColumn>> Column
    {
        set
        {
            ColumnName = value.GetMemberNameTree();
        }
    }
}

public class TableIndexDefinition
{
    /*
    {
    "definition": {
      "column": "example_column",
      "options": {
        "caseSensitive": false,
        "normalize": true,
        "ascii": false
      }
    }
  }
    */
    [JsonPropertyName("column")]
    public string ColumnName { get; set; }

    [JsonInclude]
    [JsonPropertyName("options")]
    internal Dictionary<string, string> Options { get; set; } = new Dictionary<string, string>();


    [JsonIgnore]
    public bool CaseSensitive
    {
        get { return Options.ContainsKey("caseSensitive") ? Options["caseSensitive"] == "true" : false; }
        set { Options["caseSensitive"] = value ? "true" : "false"; }
    }

    [JsonIgnore]
    public bool Normalize
    {
        get { return Options.ContainsKey("normalize") ? Options["normalize"] == "true" : false; }
        set { Options["normalize"] = value ? "true" : "false"; }
    }

    [JsonIgnore]
    public bool Ascii
    {
        get { return Options.ContainsKey("ascii") ? Options["ascii"] == "true" : false; }
        set { Options["ascii"] = value ? "true" : "false"; }
    }

}
