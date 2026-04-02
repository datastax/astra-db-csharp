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
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Represents the result of inserting multiple rows into a table.
/// </summary>
[JsonConverter(typeof(TableInsertManyResultConverter))]
public class TableInsertManyResult
{
  /// <summary>
  /// The primary key schema of the returned rows
  /// </summary>
  [JsonPropertyName("primaryKeySchema")]
  internal Dictionary<string, PrimaryKeySchema> PrimaryKeys { get; set; }

  /// <summary>
  /// A list of the Ids of the inserted documents
  /// </summary>
  [JsonPropertyName("insertedIds")]
  public List<List<object>> InsertedIdTuples { get; set; } = new();

  /// <summary>
  /// The number of documents that were inserted
  /// </summary>
  [JsonIgnore]
  public int InsertedCount => InsertedIdTuples.Count;
}
