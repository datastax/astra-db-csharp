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

using DataStax.AstraDB.DataApi.Core.Query;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Options to use when replacing documents in a collection.
/// </summary>
public class ReplaceOptions<T> where T : class
{
  [JsonInclude]
  [JsonPropertyName("options")]
  internal ReplaceOptionsParameters Parameters { get; set; } = new();

  internal Filter<T> Filter { get; set; }

  [JsonInclude]
  [JsonPropertyName("filter")]
  internal Dictionary<string, object> FilterMap => Filter == null ? null : Filter.Serialize();

  /// <summary>
  /// Defines the fields to be returned in the result.
  /// </summary>
  [JsonIgnore]
  public IProjectionBuilder Projection { get; set; }

  [JsonInclude]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  [JsonPropertyName("projection")]
  internal Dictionary<string, object> ProjectionMap => Projection == null ? null : Projection.Projections.ToDictionary(x => x.FieldName, x => x.Value);

  /// <summary>
  /// Defines the sort order to apply before making the replacement.
  /// </summary>
  [JsonIgnore]
  public SortBuilder<T> Sort { get; set; }

  [JsonInclude]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  [JsonPropertyName("sort")]
  internal Dictionary<string, object> SortMap => Sort == null ? null : Sort.Sorts.ToDictionary(x => x.Name, x => x.Value);

  /// <summary>
  /// The document to replace the matching document with.
  /// </summary>
  [JsonPropertyName("replacement")]
  public T Replacement { get; set; }

  /// <summary>
  /// Whether to insert the document if no matching document is found or not.
  /// </summary>
  [JsonIgnore]
  public bool Upsert
  {
    set => Parameters.Upsert = value;
  }

  /// <summary>
  /// Whether to return the document before or after the replacement.
  /// </summary>
  [JsonIgnore]
  public ReturnDocumentDirective? ReturnDocument
  {
    set => Parameters.ReturnDocument = value.Serialize();
  }
}

internal class ReplaceOptionsParameters
{
  [JsonInclude]
  [JsonPropertyName("upsert")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  internal bool? Upsert { get; set; }

  [JsonInclude]
  [JsonPropertyName("returnDocument")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  internal string ReturnDocument { get; set; }
}