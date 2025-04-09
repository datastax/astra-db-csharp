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
/// Base class for update options.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public abstract class UpdateOptions<T> where T : class
{
  [JsonInclude]
  [JsonPropertyName("options")]
  internal UpdateOptionsParameters Parameters { get; set; } = new();

  internal Filter<T> Filter { get; set; }

  internal UpdateBuilder<T> Update { get; set; }

  [JsonInclude]
  [JsonPropertyName("filter")]
  internal Dictionary<string, object> FilterMap => Filter == null ? null : Filter.Serialize();

  [JsonInclude]
  [JsonPropertyName("update")]
  internal Dictionary<string, object> UpdateMap => Update == null ? null : Update.Serialize();
}

internal class UpdateOptionsParameters
{
  [JsonInclude]
  [JsonPropertyName("upsert")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  internal bool? Upsert { get; set; }

  [JsonInclude]
  [JsonPropertyName("returnDocument")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  internal string ReturnDocument { get; set; }

  [JsonInclude]
  [JsonPropertyName("pageState")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  internal string NextPageState { get; set; }
}

/// <summary>
/// Options for FindOneAndUpdate operations
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class FindOneAndUpdateOptions<T> : UpdateOptions<T> where T : class
{
  /// <summary>
  /// The sort order to use when determining the document to update.
  /// </summary>
  [JsonIgnore]
  public SortBuilder<T> Sort { get; set; }

  [JsonInclude]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  [JsonPropertyName("sort")]
  internal Dictionary<string, object> SortMap => Sort == null ? null : Sort.Sorts.ToDictionary(x => x.Name, x => x.Value);

  /// <summary>
  /// The Projection to use to define the fields to return.
  /// </summary>
  [JsonIgnore]
  public IProjectionBuilder Projection { get; set; }

  [JsonInclude]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  [JsonPropertyName("projection")]
  internal Dictionary<string, object> ProjectionMap => Projection == null ? null : Projection.Projections.ToDictionary(x => x.FieldName, x => x.Value);

  /// <summary>
  /// Whether to insert a new document if the filter does not match any documents.
  /// </summary>
  [JsonIgnore]
  public bool Upsert
  {
    get => Parameters.Upsert ?? false;
    set => Parameters.Upsert = value;
  }

  /// <summary>
  /// Whether to return the original document or the updated document.
  /// </summary>
  [JsonIgnore]
  public ReturnDocumentDirective? ReturnDocument
  {
    set => Parameters.ReturnDocument = value.Serialize();
  }
}

/// <summary>
/// Options for UpdateOne operations
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class UpdateOneOptions<T> : UpdateOptions<T> where T : class
{
  /// <summary>
  /// The sort order to use when determining the document to update.
  /// </summary>
  [JsonIgnore]
  public SortBuilder<T> Sort { get; set; }

  [JsonInclude]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  [JsonPropertyName("sort")]
  internal Dictionary<string, object> SortMap => Sort == null ? null : Sort.Sorts.ToDictionary(x => x.Name, x => x.Value);

  /// <summary>
  /// Whether to insert a new document if the filter does not match any documents.
  /// </summary>
  [JsonIgnore]
  public bool Upsert
  {
    get => Parameters.Upsert ?? false;
    set => Parameters.Upsert = value;
  }
}

/// <summary>
/// Options for UpdateMany operations
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class UpdateManyOptions<T> : UpdateOptions<T> where T : class
{
  /// <summary>
  /// Whether to insert a new document if the filter does not match any documents.
  /// </summary>
  [JsonIgnore]
  public bool Upsert
  {
    get => Parameters.Upsert ?? false;
    set => Parameters.Upsert = value;
  }

  [JsonIgnore]
  internal string NextPageState
  {
    get => Parameters.NextPageState;
    set => Parameters.NextPageState = value;
  }
}