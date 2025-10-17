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
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Definition of a vector index on a table
/// </summary>
/// <typeparam name="TRow"></typeparam>
/// <typeparam name="TColumn"></typeparam>
public class TableVectorIndexDefinition<TRow, TColumn> : TableVectorIndexDefinition
{
  /// <summary>
  /// The column to create the vector index on
  /// </summary>
  public Expression<Func<TRow, TColumn>> Column
  {
    set
    {
      ColumnName = value.GetMemberNameTree();
    }
  }
}

/// <summary>
/// Definition of a vector index on a table
/// </summary>
public class TableVectorIndexDefinition
{
  /// <summary>
  /// The name of the column to create the vector index on
  /// </summary>
  [JsonPropertyName("column")]
  public string ColumnName { get; set; }

  [JsonInclude]
  [JsonPropertyName("options")]
  internal Dictionary<string, object> Options { get; set; } = new Dictionary<string, object>();

  /// <summary>
  /// The similarity metric to use
  /// </summary>
  [JsonIgnore]
  public SimilarityMetric Metric
  {
    get { return (SimilarityMetric)Options["metric"]; }
    set { Options["metric"] = value; }
  }


  /// <summary>
  /// The source model
  /// </summary>
  [JsonIgnore]
  public string SourceModel
  {
    get { return (string)Options["sourceModel"]; }
    set { Options["sourceModel"] = value; }
  }

}
