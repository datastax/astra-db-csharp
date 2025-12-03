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

using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A set of options to be used when finding rows in a table.
/// </summary>
/// <typeparam name="T"></typeparam>
public class TableFindManyOptions<T> : TableFindOptions<T>, IFindManyOptions<T, TableSortBuilder<T>>
{

    /// <summary>
    /// The number of documents to skip before starting to return documents.
    /// Use in conjuction with <see cref="Sort"/> to determine the order to apply before skipping. 
    /// </summary>
    [JsonIgnore]
    public int? Skip { get => _skip; set => _skip = value; }

    /// <summary>
    /// The number of documents to return.
    /// </summary>
    [JsonIgnore]
    public int? Limit { get => _limit; set => _limit = value; }

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
    internal bool? IncludeSortVector { get => _includeSortVector; set => _includeSortVector = value; }
    bool? IFindManyOptions<T, TableSortBuilder<T>>.IncludeSortVector { get => IncludeSortVector; set => IncludeSortVector = value; }

    IFindManyOptions<T, TableSortBuilder<T>> IFindManyOptions<T, TableSortBuilder<T>>.Clone()
    {
        var clone = new TableFindManyOptions<T>
        {
            Filter = Filter != null ? Filter.Clone() : null,
            PageState = PageState,
            Skip = Skip,
            Limit = Limit,
            IncludeSimilarity = IncludeSimilarity,
            Projection = Projection != null ? Projection.Clone() : null,
            Sort = Sort != null ? Sort.Clone() : null
        };
        return clone;
    }

}
