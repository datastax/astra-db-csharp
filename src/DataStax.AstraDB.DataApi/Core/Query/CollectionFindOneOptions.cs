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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Options for finding a single document in a collection.
/// </summary>
/// <typeparam name="T">The type of the document.</typeparam>
public class CollectionFindOneOptions<T>
{
    /// <summary>The projection to apply to the results.</summary>
    [JsonIgnore]
    public IProjectionBuilder Projection { get; set; }

    /// <summary>
    /// Whether to include a similarity score in the results or not (when performing a vector sort).
    /// </summary>
    [JsonIgnore]
    public bool? IncludeSimilarity { get; set; }

    /// <summary>The sort to apply when running the query.</summary>
    [JsonIgnore]
    public CollectionSortBuilder<T> Sort { get; set; }

    internal Filter<T> Filter { get; set; }

    [JsonInclude]
    [JsonPropertyName("filter")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal Dictionary<string, object> FilterMap => Filter == null ? null : Filter.Serialize();

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
                { "includeSimilarity", IncludeSimilarity }
            };
            options = options.Where(pair => pair.Value != null).ToDictionary(pair => pair.Key, pair => pair.Value);
            if (options.Count == 0)
            {
                return null;
            }
            return options;
        }
    }

    internal CollectionFindOneOptions<T> Clone()
    {
        return new CollectionFindOneOptions<T>
        {
            Filter = Filter != null ? Filter.Clone() : null,
            IncludeSimilarity = IncludeSimilarity,
            Projection = Projection != null ? Projection.Clone() : null,
            Sort = Sort != null ? Sort.Clone() : null
        };
    }

    internal CollectionFindOneOptions<T> WithFilterParam(CollectionFilter<T> filter)
    {
        if (filter == null)
        {
            return this;
        }
        
        if (Filter != null)
        {
            throw new ArgumentException("Cannot pass a filter both within FindOptions and as stand-alone argument");
        }
        
        var cloned = Clone();
        cloned.Filter = filter;
        return cloned;
    }
}
