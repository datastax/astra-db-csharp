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
/// A cursor for finding and enumerating documents in a collection.
/// This is a convenience class that uses the same type for both the document and result.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
/// <remarks>
/// This cursor is returned by <see cref="Collection{T, Tid}.Find(CollectionFilter{T}, CollectionFindOptions{T})"/> and provides a fluent API
/// for filtering, sorting, limiting, and projecting documents. It supports both synchronous
/// and asynchronous iteration patterns.
/// </remarks>
/// <example>
/// <code>
/// // Basic usage with foreach
/// var cursor = collection.Find()
///     .Filter(Builders&lt;MyDocument&gt;.CollectionFilter.Eq(d => d.Status, "active"))
///     .Sort(Builders&lt;MyDocument&gt;.CollectionSort.Ascending(d => d.Name))
///     .Limit(10);
/// 
/// foreach (var doc in cursor)
/// {
///     Console.WriteLine(doc.Name);
/// }
/// 
/// // Async iteration
/// await foreach (var doc in cursor)
/// {
///     await ProcessDocumentAsync(doc);
/// }
/// </code>
/// </example>
public class CollectionFindCursor<T> : CollectionFindCursor<T, T> where T : class
{
    internal CollectionFindCursor(Filter<T> filter, BaseFindOptions<T, CollectionSortBuilder<T>> options, FetchPageFunc<T, CollectionFindCursor<T, T>> fetchPage) 
        : base(filter, options, fetchPage) { }
}

/// <summary>
/// A cursor for finding and enumerating documents in a collection with projection support.
/// This class allows you to specify a different result type than the document type, useful for projections.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (e.g., when using projections).</typeparam>
/// <remarks>
/// This cursor is returned by <see cref="Collection{T, TId}.Find{TResult}(CollectionFilter{T}, CollectionFindOptions{T})"/> and provides a fluent API
/// for filtering, sorting, limiting, and projecting documents into a different result type.
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
/// var cursor = collection.Find&lt;MyDocumentProjection&gt;()
///     .Project(Builders&lt;MyDocument&gt;.Projection
///         .Include(d => d.Name)
///         .Include(d => d.Email))
///     .Limit(100);
/// 
/// var results = await cursor.ToListAsync();
/// </code>
/// </example>
public class CollectionFindCursor<T, TResult> : FindCursor<T, TResult, CollectionSortBuilder<T>, CollectionFindCursor<T, TResult>>
    where T : class
    where TResult : class
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CollectionFindCursor{T, TResult}"/> class.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <param name="fetchPage">The function to fetch pages of results.</param>
    internal CollectionFindCursor(
        Filter<T> filter,
        BaseFindOptions<T, CollectionSortBuilder<T>> options,
        FetchPageFunc<TResult, CollectionFindCursor<T, TResult>> fetchPage
    ) : base(filter, options ?? new CollectionFindOptions<T>(), fetchPage) { }

    /// <summary>
    /// Creates a new cursor instance with the same configuration.
    /// </summary>
    /// <returns>A new cursor instance.</returns>
    public override CollectionFindCursor<T, TResult> Clone()
    {
        return new(CurrentFilter, FindOptions, FetchPageFunc);
    }

    /// <summary>
    /// Creates a new cursor instance with updated filter and options.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <returns>A new cursor instance.</returns>
    protected override CollectionFindCursor<T, TResult> CloneWith(Filter<T> filter, BaseFindOptions<T, CollectionSortBuilder<T>> options)
    {
        return new(filter, options, FetchPageFunc);
    }
}
