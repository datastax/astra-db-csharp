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

public class TableVectorIndexDefinition<TRow, TColumn> : TableVectorIndexDefinition
{
  public Expression<Func<TRow, TColumn>> Column
  {
    set
    {
      ColumnName = value.GetMemberNameTree();
    }
  }
}

public class TableVectorIndexDefinition
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
  public string Metric
  {
    get { return Options["metric"]; }
    set { Options["metric"] = value; }
  }

  [JsonIgnore]
  public string SourceModel
  {
    get { return Options["sourceModel"]; }
    set { Options["sourceModel"] = value; }
  }

}
