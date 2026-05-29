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
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Enumeration;

/// <summary>
/// A fluent API cursor for finding and enumerating records or rows with filtering, sorting, and projection capabilities.
/// 
/// This cursor extends <see cref="PaginatedCursor{T,TResult,TOptions,TCursor}"/> to provide query-specific operations like skipping or sorting.
///
/// It supports both synchronous and asynchronous iteration patterns.
/// 
/// Use the fluent methods to refine your query, then iterate using foreach, LINQ, or manual cursor navigation.
/// </summary>
/// <typeparam name="T">The type representing the record or row being queried.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (e.g., when using projections).</typeparam>
/// <typeparam name="TSort">The type of sort builder to use (e.g., <see cref="CollectionSortBuilder{T}"/> or <see cref="TableSortBuilder{T}"/>).</typeparam>
/// <typeparam name="TCursor">The concrete cursor type for fluent method chaining.</typeparam>
public abstract class FindCursor<T, TResult, TSort, TCursor> : PaginatedCursor<T, TResult, BaseFindOptions<T, TSort>, TCursor>
    where T : class
    where TResult : class
    where TSort : SortBuilder<T>
    where TCursor : FindCursor<T, TResult, TSort, TCursor>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FindCursor{T, TResult, TSort, TCursor}"/> class.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <param name="fetchPageFunc">The function to fetch pages of results.</param>
    internal FindCursor(Filter<T> filter, BaseFindOptions<T, TSort> options,
        FetchPageFunc<TResult, TCursor> fetchPageFunc) : base(filter, options, fetchPageFunc)
    {
    }

    /// <summary>
    /// Specifies a sort to apply to the query results.
    /// </summary>
    /// <param name="sort">The sort to apply.</param>
    /// <returns>A new cursor instance with the updated sort.</returns>
    /// <example>
    /// <code>
    /// var sort = Builders&lt;MyRecord&gt;.CollectionSort.Ascending(d => d.Name);
    /// var cursor = collection.Find().Sort(sort);
    /// </code>
    /// </example>
    public TCursor Sort(TSort sort)
    {
        return UpdateOptions(options => options.Sort = sort);
    }

    /// <summary>
    /// Specifies the number of records to skip before starting to return records.
    /// Use in conjunction with <see cref="Sort"/> to determine the order before skipping.
    /// </summary>
    /// <param name="skip">The number of records to skip.</param>
    /// <returns>A new cursor instance with the updated skip value.</returns>
    public TCursor Skip(int skip)
    {
        return UpdateOptions(options => options.Skip = skip);
    }

    /// <summary>
    /// Specifies whether to include the similarity score in the results.
    /// </summary>
    /// <param name="include">Whether to include the similarity score. Defaults to true.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <remarks>
    /// Use the <see cref="SerDes.DocumentMappingAttribute"/> with <see cref="SerDes.DocumentMappingField.Similarity"/>
    /// to map the similarity score to a property in your result class.
    /// </remarks>
    /// <example>
    /// <code>
    /// public class MyDocumentWithSimilarity : MyDocument
    /// {
    ///     [DocumentMapping(DocumentMappingField.Similarity)]
    ///     public double? Similarity { get; set; }
    /// }
    /// 
    /// var cursor = collection.Find&lt;MyDocumentWithSimilarity&gt;()
    ///     .Sort(Builders&lt;MyDocument&gt;.CollectionSort.Vector(vectorQuery))
    ///     .IncludeSimilarity();
    /// </code>
    /// </example>
    public TCursor IncludeSimilarity(bool include = true)
    {
        return UpdateOptions(options => options.IncludeSimilarity = include);
    }

    internal override TCursor UpdateOptions(Action<BaseFindOptions<T, TSort>> optionsUpdater)
    {
        if (State != CursorState.Idle)
        {
            throw new CursorException("Cursors must be idle when building their options", State);
        }
        var newOptions = FindOptions.ShallowClone();
        optionsUpdater(newOptions);
        return CloneWith(CurrentFilter, newOptions);
    }
}
