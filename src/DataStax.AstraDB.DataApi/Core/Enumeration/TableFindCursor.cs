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
using DataStax.AstraDB.DataApi.Tables;

namespace DataStax.AstraDB.DataApi.Core.Enumeration;

/// <summary>
/// A cursor for finding and enumerating rows in a table.
/// This is a convenience class that uses the same type for both the row and result.
/// </summary>
/// <typeparam name="T">The type of the rows in the table.</typeparam>
/// <remarks>
/// This cursor is returned by <see cref="Table{T}.Find(TableFindOptions{T})"/> and provides a fluent API
/// for filtering, sorting, limiting, and projecting rows. It supports both synchronous
/// and asynchronous iteration patterns.
/// </remarks>
/// <example>
/// <code>
/// // Basic usage with foreach
/// var cursor = table.Find()
///     .Filter(Builders&lt;MyRow&gt;.TableFilter.Eq(r => r.Status, "active"))
///     .Sort(Builders&lt;MyRow&gt;.TableSort.Ascending(r => r.Name))
///     .Limit(10);
/// 
/// foreach (var row in cursor)
/// {
///     Console.WriteLine(row.Name);
/// }
/// 
/// // Async iteration
/// await foreach (var row in cursor)
/// {
///     await ProcessRowAsync(row);
/// }
/// </code>
/// </example>
public class TableFindCursor<T> : TableFindCursor<T, T> where T : class
{
    internal TableFindCursor(Filter<T> filter, BaseFindOptions<T, TableSortBuilder<T>> options, FetchPageFunc<T, TableFindCursor<T, T>> fetchPage) 
        : base(filter, options, fetchPage) { }
}

/// <summary>
/// A cursor for finding and enumerating rows in a table with projection support.
/// This class allows you to specify a different result type than the row type, useful for projections.
/// </summary>
/// <typeparam name="T">The type of the rows in the table.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (e.g., when using projections).</typeparam>
/// <remarks>
/// This cursor is returned by <see cref="Table{T}.Find{TResult}(TableFindOptions{T})"/> and provides a fluent API
/// for filtering, sorting, limiting, and projecting rows into a different result type.
/// </remarks>
/// <example>
/// <code>
/// // Using projection to return only specific fields
/// public class MyRowProjection
/// {
///     public string Name { get; set; }
///     public string Email { get; set; }
/// }
/// 
/// var cursor = table.Find&lt;MyRowProjection&gt;()
///     .Project(Builders&lt;MyRow&gt;.Projection
///         .Include(r => r.Name)
///         .Include(r => r.Email))
///     .Limit(100);
/// 
/// var results = await cursor.ToListAsync();
/// </code>
/// </example>
public class TableFindCursor<T, TResult> : FindCursor<T, TResult, TableSortBuilder<T>, TableFindCursor<T, TResult>>
    where T : class
    where TResult : class
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TableFindCursor{T, TResult}"/> class.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <param name="fetchPage">The function to fetch pages of results.</param>
    internal TableFindCursor(
        Filter<T> filter,
        BaseFindOptions<T, TableSortBuilder<T>> options,
        FetchPageFunc<TResult, TableFindCursor<T, TResult>> fetchPage
    ) : base(filter, options ?? new TableFindOptions<T>(), fetchPage) { }

    /// <summary>
    /// Creates a new cursor instance with the same configuration.
    /// </summary>
    /// <returns>A new cursor instance.</returns>
    public override TableFindCursor<T, TResult> Clone()
    {
        return new(CurrentFilter, FindOptions, FetchPageFunc);
    }

    /// <summary>
    /// Creates a new cursor instance with updated filter and options.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <returns>A new cursor instance.</returns>
    protected override TableFindCursor<T, TResult> CloneWith(Filter<T> filter, BaseFindOptions<T, TableSortBuilder<T>> options)
    {
        return new(filter, options, FetchPageFunc);
    }
}
