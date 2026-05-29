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

using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core.Query;

namespace DataStax.AstraDB.DataApi.Core.Enumeration;

/// <summary>
/// A cursor for running a find-and-rerank query on a collection with projection support.
/// This is a convenience class that uses the same type for both the row and result.
/// </summary>
/// <typeparam name="T">The type of the rows in the table.</typeparam>
/// <remarks>
/// This cursor is returned by <see cref="Collection{T, TId}.FindAndRerank(CollectionFilter{T},CollectionFindAndRerankOptions{T})"/> and provides a fluent API
/// for applying settings such as filtering, sorting, limiting and projecting rows. It supports both synchronous
/// and asynchronous iteration patterns.
/// </remarks>
/// <example>
/// <code>
/// // Basic usage with foreach
/// var cursor = collection.FindAndRerank()
///      .Sort(
///         Builders&lt;MyDocument&gt;.CollectionFindAndRerankSort.Hybrid(
///             "a tree on a grassy hillside"
///         )
///     )
///     .Limit(20);
///
/// foreach (var item in cursor)
/// {
///     Console.WriteLine(item.Document.Name);
/// }
/// 
/// // Async iteration
/// await foreach (var item in cursor)
/// {
///     await ProcessRowAsync(item.Document);
/// }
/// </code>
/// </example>
public class CollectionFindAndRerankCursor<T> : CollectionFindAndRerankCursor<T, RerankedResult<T>> where T : class
{
    internal CollectionFindAndRerankCursor(Filter<T> filter, BaseFindAndRerankOptions<T, CollectionFindAndRerankSortBuilder<T>> options, FetchPageFunc<RerankedResult<T>, CollectionFindAndRerankCursor<T, RerankedResult<T>>> fetchPage)
        : base(filter, options, fetchPage) { }
}

/// <summary>
/// A cursor for running a find-and-rerank query on a collection with projection support.
/// This class allows you to specify a different result type than the row type, useful for projections.
/// </summary>
/// <typeparam name="T">The type of the rows in the table.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (e.g., when using projections).</typeparam>
/// <remarks>
/// This cursor is returned by <see cref="Collection{T, TId}.FindAndRerank{TResult}(CollectionFilter{T}, CollectionFindAndRerankOptions{T})"/> and provides a fluent API
/// for applying settings such as filtering, sorting, limiting, including projecting rows into a different result type.
/// It supports both synchronous and asynchronous iteration patterns.
/// </remarks>
/// <example>
/// <code>
/// // Using projection to return only specific fields
/// public class MyDocumentProjection
/// {
///     public string Name { get; set; }
///     public string Email { get; set; }
/// }
/// 
/// var cursor = collection.FindAndRerank&lt;MyDocumentProjection&gt;()
///      .Sort(
///         Builders&lt;MyDocument&gt;.CollectionFindAndRerankSort.Hybrid(
///             "a tree on a grassy hillside"
///         )
///     )
///     .Project(Builders&lt;MyDocument&gt;.Projection
///         .Include(d => d.Name)
///         .Include(d => d.Email))
///     .Limit(20);
/// 
/// var results = await cursor.ToListAsync();
/// var firstDocument = results[0].Document;
/// </code>
/// </example>
public class CollectionFindAndRerankCursor<T, TResult> : FindAndRerankCursor<T, TResult, CollectionFindAndRerankSortBuilder<T>, CollectionFindAndRerankCursor<T, TResult>>
    where T : class
    where TResult : class
{
    internal CollectionFindAndRerankCursor(
        Filter<T> filter,
        BaseFindAndRerankOptions<T, CollectionFindAndRerankSortBuilder<T>> options,
        FetchPageFunc<TResult, CollectionFindAndRerankCursor<T, TResult>> fetchPage
    ) : base(filter, options ?? new CollectionFindAndRerankOptions<T>(), fetchPage) { }

    /// <summary>
    /// Creates a new cursor instance with the same configuration.
    /// </summary>
    /// <returns>A new cursor instance.</returns>
    public override CollectionFindAndRerankCursor<T, TResult> Clone()
    {
        return new(CurrentFilter, FindOptions, FetchPageFunc);
    }

    /// <summary>
    /// Creates a new cursor instance with updated filter and options.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <returns>A new cursor instance.</returns>
    protected override CollectionFindAndRerankCursor<T, TResult> CloneWith(Filter<T> filter, BaseFindAndRerankOptions<T, CollectionFindAndRerankSortBuilder<T>> options)
    {
        return new(filter, options, FetchPageFunc);
    }
}
