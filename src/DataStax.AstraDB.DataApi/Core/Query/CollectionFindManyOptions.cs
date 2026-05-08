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
/// Options for finding multiple documents in a collection.
/// </summary>
/// <typeparam name="T">The type of the document.</typeparam>
public class CollectionFindManyOptions<T> : CommandOptions, IFindManyOptions<T, CollectionSortBuilder<T>>
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

    /// <summary>
    /// The initial page state used to resume pagination from a previous find-many operation.
    /// </summary>
    [JsonIgnore]
    public string InitialPageState { get => PageState; set => PageState = value; }

    /// <summary>An optional amount of documents to skip in a find operation.</summary>
    [JsonIgnore]
    public int? Skip { get; set; }

    /// <summary>An optional maximum number of documents to return in a find operation.</summary>
    [JsonIgnore]
    public int? Limit { get; set; }

    [JsonIgnore]
    internal bool? IncludeSortVector { get; set; }

    bool? IFindManyOptions<T, CollectionSortBuilder<T>>.IncludeSortVector { get => IncludeSortVector; set => IncludeSortVector = value; }

    internal Filter<T> Filter { get; set; }

    [JsonIgnore]
    internal string PageState { get; set; }

    string IFindOptions<T, CollectionSortBuilder<T>>.PageState { get => PageState; set => PageState = value; }

    Filter<T> IFindOptions<T, CollectionSortBuilder<T>>.Filter { get => Filter; set => Filter = value; }

    [JsonInclude]
    [JsonPropertyName("filter")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal Dictionary<string, object> FilterMap => Filter == null ? null : Filter.Serialize();

    CollectionSortBuilder<T> IFindOptions<T, CollectionSortBuilder<T>>.Sort { get => Sort; set => Sort = value; }

    IProjectionBuilder IFindOptions<T, CollectionSortBuilder<T>>.Projection { get => Projection; set => Projection = value; }

    bool? IFindOptions<T, CollectionSortBuilder<T>>.IncludeSimilarity { get => IncludeSimilarity; set => IncludeSimilarity = value; }

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
                { "includeSortVector", IncludeSortVector },
                { "pageState", PageState },
                { "skip", Skip },
                { "limit", Limit },
            };
            options = options.Where(pair => pair.Value != null).ToDictionary(pair => pair.Key, pair => pair.Value);
            if (options.Count == 0)
            {
                return null;
            }
            return options;
        }
    }

    IFindManyOptions<T, CollectionSortBuilder<T>> IFindManyOptions<T, CollectionSortBuilder<T>>.PayloadOptions()
    {
        return new CollectionFindManyOptions<T> {
            Filter = Filter,
            InitialPageState = InitialPageState,
            Skip = Skip,
            Limit = Limit,
            IncludeSortVector = IncludeSortVector,
            IncludeSimilarity = IncludeSimilarity,
            Projection = Projection != null ? Projection.Clone() : null,
            Sort = Sort != null ? Sort.Clone() : null,
        };
    }

    internal CommandOptions CommandOptions()
    {
        return new CommandOptions {
            Token = Token,
            RunMode = RunMode,
            Destination = Destination,
            HttpClientOptions = HttpClientOptions != null ? HttpClientOptions.Clone() : null,
            TimeoutOptions = TimeoutOptions != null ? TimeoutOptions.Clone() : null,
            ApiVersion = ApiVersion,
            CancellationToken = CancellationToken
        };
    }

    internal CollectionFindManyOptions<T> Clone()
    {
        return new CollectionFindManyOptions<T>
        {
            Filter = Filter != null ? Filter.Clone() : null,
            InitialPageState = InitialPageState,
            Skip = Skip,
            Limit = Limit,
            IncludeSortVector = IncludeSortVector,
            IncludeSimilarity = IncludeSimilarity,
            Projection = Projection != null ? Projection.Clone() : null,
            Sort = Sort != null ? Sort.Clone() : null,
            // CommandOptions properties:
            Token = Token,
            RunMode = RunMode,
            Destination = Destination,
            HttpClientOptions = HttpClientOptions != null ? HttpClientOptions.Clone() : null,
            TimeoutOptions = TimeoutOptions != null ? TimeoutOptions.Clone() : null,
            ApiVersion = ApiVersion,
            CancellationToken = CancellationToken
        };
    }

    IFindManyOptions<T, CollectionSortBuilder<T>> IFindManyOptions<T, CollectionSortBuilder<T>>.Clone()
    {
        return Clone();
    }

    internal CollectionFindManyOptions<T> WithFilterParam(CollectionFilter<T> filter)
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
