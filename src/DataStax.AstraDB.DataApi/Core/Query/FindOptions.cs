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
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Base class for find operation options, providing projection and sort configuration.
/// </summary>
/// <typeparam name="T">The type of the document or row.</typeparam>
/// <typeparam name="TSort">The type of the sort builder.</typeparam>
public abstract class FindOptions<T, TSort> : IFindOptions<T, TSort> where TSort : SortBuilder<T>
{
    /// <summary>The projection to apply to the results.</summary>
    [JsonIgnore]
    public IProjectionBuilder Projection { get; set; }

    /// <summary>
    /// Whether to include a similarity score in the results or not (when performing a vector sort).
    /// </summary>
    [JsonIgnore]
    public bool? IncludeSimilarity { get; set; }

    /// <summary>When <see langword="true"/>, the sort vector is included in the response.</summary>
    protected bool? _includeSortVector;

    /// <summary>Number of results to skip before returning documents.</summary>
    protected int? _skip;

    /// <summary>Maximum number of documents to return.</summary>
    protected int? _limit;

    internal Filter<T> Filter { get; set; }

    /// <summary>The sort to apply to the results.</summary>
    [JsonIgnore]
    public abstract TSort Sort { get; set; }

    [JsonIgnore]
    internal string PageState { get; set; }
    string IFindOptions<T, TSort>.PageState { get => PageState; set => PageState = value; }

    [JsonInclude]
    [JsonPropertyName("filter")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal Dictionary<string, object> FilterMap => Filter == null ? null : Filter.Serialize();

    Filter<T> IFindOptions<T, TSort>.Filter { get => Filter; set => Filter = value; }

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("sort")]
    internal Dictionary<string, object> SortMap => Sort == null ? null : Sort.Sorts.ToDictionary(x => x.Name, x => x.Value);

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("projection")]
    internal Dictionary<string, object> ProjectionMap => Projection == null ? null : Projection.Projections.ToDictionary(x => x.FieldName, x => x.Value);

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("options")]
    internal Dictionary<string, object> Options
    {
        get
        {
            var options = new Dictionary<string, object>()
            {
                { "includeSimilarity", IncludeSimilarity },
                { "includeSortVector", _includeSortVector },
                { "pageState", PageState },
                { "skip", _skip },
                { "limit", _limit }
            };
            options = options.Where(pair => pair.Value != null).ToDictionary(pair => pair.Key, pair => pair.Value);
            if (options.Count == 0)
            {
                return null;
            }
            return options;
        }
    }

    [JsonIgnore]
    TSort IFindOptions<T, TSort>.Sort { get => Sort; set => Sort = value; }
    [JsonIgnore]
    IProjectionBuilder IFindOptions<T, TSort>.Projection { get => Projection; set => Projection = value; }
    [JsonIgnore]
    bool? IFindOptions<T, TSort>.IncludeSimilarity { get => IncludeSimilarity; set => IncludeSimilarity = value; }

}