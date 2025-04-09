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

using MongoDB.Bson;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A set of options to be used when finding documents in a collection.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class FindOptions<T>
{
    /// <summary>
    /// The builder used to define the sort to apply when running the query.
    /// </summary>
    /// <example>
    /// <code>
    /// // Sort documents by the nested Properties.PropertyOne field in ascending order
    /// var sortBuilder = Builders&lt;SimpleObject&gt;.Sort;
    /// var sort = sortBuilder.Ascending(so =&gt; so.Properties.PropertyOne);
    /// </code>
    /// </example>
    [JsonIgnore]
    public SortBuilder<T> Sort { get; set; }

    /// <summary>
    /// The builder used to define the Projection to apply when running the query.
    /// </summary>
    /// <example>
    /// <code>
    /// // Inclusive Projection, return only the nested Properties.PropertyOne field
    /// var projectionBuilder = Builders&lt;SimpleObject&gt;.Projection;
    /// var projection = projectionBuilder.Include(p =&gt; p.Properties.PropertyOne);
    /// </code>
    /// </example>
    [JsonIgnore]
    public IProjectionBuilder Projection { get; set; }

    /// <summary>
    /// The number of documents to skip before starting to return documents.
    /// Use in conjuction with <see cref="Sort"/> to determine the order to apply before skipping. 
    /// </summary>
    [JsonIgnore]
    public int? Skip { get; set; }

    /// <summary>
    /// The number of documents to return.
    /// </summary>
    [JsonIgnore]
    public int? Limit { get; set; }

    /// <summary>
    /// Whether to include the similarity score in the result or not
    /// </summary>
    /// <example>
    /// <code>
    /// You can use the attribute <see cref="SerDes.DocumentMappingAttribute"/> to map the similarity score to the result class.
    /// public class SimpleObjectWithVectorizeResult : SimpleObjectWithVectorize
    /// {
    ///     [DocumentMapping(DocumentMappingField.Similarity)]
    ///     public double? Similarity { get; set; }
    /// }
    /// 
    /// var finder = collection.Find&lt;SimpleObjectWithVectorizeResult&gt;(
    ///     new FindOptions&lt;SimpleObjectWithVectorize&gt;() { 
    ///         Sort = Builders&lt;SimpleObjectWithVectorize&gt;.Sort.Vectorize(dogQueryVectorString), 
    ///         IncludeSimilarity = true 
    ///     }, null);
    /// var cursor = finder.ToCursor();
    /// var list = cursor.ToList();
    /// var result = list.First();
    /// var similarity = result.Similarity;
    /// </code>
    /// </example>
    [JsonIgnore]
    public bool? IncludeSimilarity { get; set; }

    /// <summary>
    /// Whether to include the sort vector in the result or not
    /// </summary>
    /// <example>
    /// <code>
    /// You can use the attribute <see cref="SerDes.DocumentMappingAttribute"/> to map the sort vector to the result class.
    /// public class SimpleObjectWithVectorizeResult : SimpleObjectWithVectorize
    /// {
    ///     [DocumentMapping(DocumentMappingField.SortVector)]
    ///     public double? SortVector { get; set; }
    /// }
    /// 
    /// var finder = collection.Find&lt;SimpleObjectWithVectorizeResult&gt;(
    ///     new FindOptions&lt;SimpleObjectWithVectorize&gt;() { 
    ///         Sort = Builders&lt;SimpleObjectWithVectorize&gt;.Sort.Vectorize(dogQueryVectorString), 
    ///         IncludeSortVector = true 
    ///     }, null);
    /// var cursor = finder.ToCursor();
    /// var list = cursor.ToList();
    /// var result = list.First();
    /// var sortVector = result.SortVector;
    /// </code>
    /// </example>
    [JsonIgnore]
    public bool? IncludeSortVector { get; set; }

    [JsonIgnore]
    internal Filter<T> Filter { get; set; }

    [JsonIgnore]
    internal string PageState { get; set; }

    [JsonInclude]
    [JsonPropertyName("filter")]
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
    internal FindApiOptions Options
    {
        get
        {
            if (IncludeSimilarity == null &&
                IncludeSortVector == null &&
                PageState == null &&
                Skip == null &&
                Limit == null &&
                string.IsNullOrEmpty(PageState))
            {
                return null;
            }
            return new FindApiOptions
            {
                IncludeSimilarity = IncludeSimilarity,
                IncludeSortVector = IncludeSortVector,
                PageState = PageState,
                Skip = Skip,
                Limit = Limit
            };
        }
    }


}

internal class FindApiOptions
{
    [JsonPropertyName("skip")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Skip { get; set; }

    [JsonPropertyName("limit")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Limit { get; set; }

    [JsonPropertyName("includeSimilarity")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? IncludeSimilarity { get; set; }

    [JsonPropertyName("includeSortVector")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? IncludeSortVector { get; set; }

    [JsonPropertyName("pageState")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string PageState { get; set; }
}
