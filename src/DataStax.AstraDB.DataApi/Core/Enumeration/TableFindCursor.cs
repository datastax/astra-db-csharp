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
/// This cursor is returned by <see cref="Table{T}.Find()"/> and provides a fluent API
/// for filtering, sorting, limiting, and projecting rows. It supports both synchronous
/// and asynchronous iteration patterns.
/// </remarks>
/// <example>
/// <code>
/// // Basic usage with foreach
/// var cursor = table.Find()
///     .Filter(Builders&lt;MyRow&gt;.Filter.Eq(r => r.Status, "active"))
///     .Sort(Builders&lt;MyRow&gt;.Sort.Ascending(r => r.CreatedAt))
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
    internal TableFindCursor(IFindManyOptions<T, TableSortBuilder<T>> options, CommandOptions commandOptions, FetchPageFunc<T, TableFindCursor<T, T>> fetchPage) 
        : base(options, commandOptions, fetchPage) { }
}

/// <summary>
/// A cursor for finding and enumerating rows in a table with projection support.
/// This class allows you to specify a different result type than the row type, useful for projections.
/// </summary>
/// <typeparam name="T">The type of the rows in the table.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (e.g., when using projections).</typeparam>
/// <remarks>
/// This cursor is returned by <see cref="Table{T}.Find{TResult}()"/> and provides a fluent API
/// for filtering, sorting, limiting, and projecting rows into a different result type.
/// </remarks>
/// <example>
/// <code>
/// // Using projection to return only specific columns
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
    /// <param name="options">The find options to use.</param>
    /// <param name="commandOptions">The command options to use.</param>
    /// <param name="fetchPage">The function to fetch pages of results.</param>
    internal TableFindCursor(
        IFindManyOptions<T, TableSortBuilder<T>> options,
        CommandOptions commandOptions,
        FetchPageFunc<TResult, TableFindCursor<T, TResult>> fetchPage
    ) : base(options, commandOptions, fetchPage)
    {
    }

    /// <summary>
    /// Creates a new cursor instance with the same configuration.
    /// </summary>
    /// <returns>A new cursor instance.</returns>
    public override AbstractCursor<TResult> Clone()
    {
        return new TableFindCursor<T, TResult>(FindOptions.Clone(), CommandOptions, FetchPageFunc);
    }

    /// <summary>
    /// Creates a new cursor instance with updated find options.
    /// </summary>
    /// <param name="options">The updated find options.</param>
    /// <returns>A new cursor instance with the updated options.</returns>
    internal override TableFindCursor<T, TResult> CloneWithOptions(IFindManyOptions<T, TableSortBuilder<T>> options)
    {
        return new TableFindCursor<T, TResult>(options, CommandOptions, FetchPageFunc);
    }
}
