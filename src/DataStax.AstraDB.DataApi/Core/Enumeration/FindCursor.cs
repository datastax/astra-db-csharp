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
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Enumeration;

/// <summary>
/// Represents a page of results from a find operation.
/// </summary>
/// <typeparam name="T">The type of records in the page.</typeparam>
public class FindPage<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FindPage{T}"/> class.
    /// </summary>
    /// <param name="nextPageState">The page state token for fetching the next page, or null if this is the last page.</param>
    /// <param name="result">The records in this page.</param>
    /// <param name="sortVector">The sort vector used for vector similarity searches, if applicable.</param>
    public FindPage(string nextPageState, List<T> result, float[] sortVector)
    {
        NextPageState = nextPageState;
        Results = new List<T>(result);
        SortVector = sortVector;
    }
    
    /// <summary>
    /// Gets the page state token for fetching the next page, or null if this is the last page.
    /// </summary>
    public string NextPageState { get; }
    
    /// <summary>
    /// Gets the list of records in this page.
    /// </summary>
    public List<T> Results { get; internal set; }
    
    /// <summary>
    /// Gets the sort vector used for vector similarity searches, if applicable.
    /// </summary>
    public float[] SortVector { get; }
}

/// <summary>
/// Delegate for fetching a page of results.
/// </summary>
/// <typeparam name="T">The type of records in the page.</typeparam>
/// <typeparam name="TCursor">The type of the cursor.</typeparam>
/// <param name="cursor">The cursor instance.</param>
/// <param name="nextPageState">A pagination token (string) for the page to fetch. When provided it usually comes from the previous page response.</param>
/// <param name="runSynchronously">Whether to run the operation synchronously.</param>
/// <returns>A task that returns the fetched page.</returns>
delegate Task<FindPage<T>> FetchPageFunc<T, in TCursor>(TCursor cursor, string nextPageState, bool runSynchronously);

/// <summary>
/// A fluent API cursor for finding and enumerating records or rows with filtering, sorting, and projection capabilities.
/// 
/// This cursor extends <see cref="AbstractCursor{T, TCursor}"/> to provide query-specific operations like filtering, sorting,
/// limiting, skipping, and projecting results. It supports both synchronous and asynchronous iteration patterns.
/// 
/// Use the fluent methods to refine your query, then iterate using foreach, LINQ, or manual cursor navigation.
/// </summary>
/// <typeparam name="T">The type representing the record or row being queried.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (e.g., when using projections).</typeparam>
/// <typeparam name="TSort">The type of sort builder to use (e.g., <see cref="CollectionSortBuilder{T}"/> or <see cref="TableSortBuilder{T}"/>).</typeparam>
/// <typeparam name="TCursor">The concrete cursor type for fluent method chaining.</typeparam>
public abstract class FindCursor<T, TResult, TSort, TCursor> : AbstractCursor<TResult, TCursor>
    where T : class
    where TResult : class
    where TSort : SortBuilder<T>
    where TCursor : FindCursor<T, TResult, TSort, TCursor>
{
    /// <summary>
    /// Gets the find options used for this cursor.
    /// </summary>
    internal BaseFindManyOptions<T, TSort> FindOptions { get; }
    
    /// <summary>
    /// Gets the filter for this cursor.
    /// </summary>
    internal Filter<T> CurrentFilter { get; private set; }
    
    /// <summary>
    /// Gets the function used to fetch pages of results.
    /// </summary>
    internal readonly FetchPageFunc<TResult, TCursor> FetchPageFunc;
    
    private FindPage<TResult> _currentPage;
    
    /// <summary>
    /// Gets the internal buffer containing the current page of results.
    /// </summary>
    protected override List<TResult> _buffer => _currentPage?.Results;
    
    /// <summary>
    /// Initializes a new instance of the <see cref="FindCursor{T, TResult, TSort, TCursor}"/> class.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <param name="fetchPage">The function to fetch pages of results.</param>
    internal FindCursor(Filter<T> filter, BaseFindManyOptions<T, TSort> options, FetchPageFunc<TResult, TCursor> fetchPage) 
    {
        CurrentFilter = filter;
        FindOptions = options;
        FetchPageFunc = fetchPage;

        if (options.InitialPageState != null)
        {
            _currentPage = new FindPage<TResult>(options.InitialPageState, new List<TResult>(), null);
        }
    }
    
    /// <summary>
    /// Specifies a filter to apply to the query.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <returns>A new cursor instance with the updated filter.</returns>
    /// <example>
    /// <code>
    /// var filter = Builders&lt;MyRecord&gt;.Filter.Eq(d => d.Status, "active");
    /// var cursor = collection.Find().Filter(filter);
    /// </code>
    /// </example>
    public TCursor Filter(Filter<T> filter)
    {
        if (State != CursorState.Idle)
        {
            throw new CursorException("Cursors must be idle when building their options", State);
        }
        return CloneWith(filter, FindOptions);
    }
    
    /// <summary>
    /// Specifies a sort to apply to the query results.
    /// </summary>
    /// <param name="sort">The sort to apply.</param>
    /// <returns>A new cursor instance with the updated sort.</returns>
    /// <example>
    /// <code>
    /// var sort = Builders&lt;MyRecord&gt;.Sort.Ascending(d => d.Name);
    /// var cursor = collection.Find().Sort(sort);
    /// </code>
    /// </example>
    public TCursor Sort(TSort sort)
    {
        return UpdateOptions(options => options.Sort = sort);
    }
    
    /// <summary>
    /// Specifies the maximum number of records to return.
    /// </summary>
    /// <param name="limit">The maximum number of records to return.</param>
    /// <returns>A new cursor instance with the updated limit.</returns>
    public TCursor Limit(int limit)
    {
        return UpdateOptions(options => options.Limit = limit);
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
    /// Specifies a projection to apply to the results.
    /// </summary>
    /// <param name="projection">The projection to apply.</param>
    /// <returns>A new cursor instance with the updated projection.</returns>
    /// <example>
    /// <code>
    /// var projection = Builders&lt;MyRecord&gt;.Projection.Include(d => d.Name).Include(d => d.Email);
    /// var cursor = collection.Find().Project(projection);
    /// </code>
    /// </example>
    public TCursor Project(IProjectionBuilder projection)
    {
        return UpdateOptions(options => options.Projection = projection);
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
    ///     .Sort(Builders&lt;MyDocument&gt;.Sort.Vector(vectorQuery))
    ///     .IncludeSimilarity();
    /// </code>
    /// </example>
    public TCursor IncludeSimilarity(bool include = true)
    {
        return UpdateOptions(options => options.IncludeSimilarity = include);
    }
    
    /// <summary>
    /// Specifies whether to include the sort vector in the results.
    /// </summary>
    /// <param name="include">Whether to include the sort vector. Defaults to true.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <remarks>
    /// When enabled, you can retrieve the sort vector using <see cref="GetSortVector()"/> or <see cref="GetSortVectorAsync(CancellationToken)"/>.
    /// This is useful for vector similarity searches where you want to access the vector used for sorting.
    /// </remarks>
    /// <example>
    /// <code>
    /// var cursor = collection.Find()
    ///     .Sort(Builders&lt;MyRecord&gt;.Sort.Vectorize("search query"))
    ///     .IncludeSortVector();
    /// 
    /// await foreach (var doc in cursor)
    /// {
    ///     // Process records
    /// }
    /// 
    /// var sortVector = await cursor.GetSortVectorAsync();
    /// </code>
    /// </example>
    public TCursor IncludeSortVector(bool include = true)
    {
        return UpdateOptions(options => options.IncludeSortVector = include);
    }
    
    /// <summary>
    /// Sets the initial page state used to resume pagination from a previous find operation.
    /// </summary>
    /// <param name="initialPageState">The page state to resume from, or null to start from the beginning.</param>
    /// <returns>A new cursor instance with the updated initial page state.</returns>
    public TCursor InitialPageState(string initialPageState)
    {
        return UpdateOptions(options => options.InitialPageState = initialPageState);
    }
    
    /// <summary>
    /// Synchronously fetches the next complete page of results from the server.
    /// </summary>
    /// <returns>The next page of results.</returns>
    /// <exception cref="CursorException">Thrown when the cursor is closed or the current buffer is not empty.</exception>
    public FindPage<TResult> FetchNextPage()
    {
        return FetchNextPageAsync(CancellationToken.None, true).ResultSync();
    }
    
    /// <summary>
    /// Asynchronously fetches the next complete page of results from the server.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to cancel the operation.</param>
    /// <returns>The next page of results.</returns>
    /// <exception cref="CursorException">Thrown when the cursor is closed or the current buffer is not empty.</exception>
    public async Task<FindPage<TResult>> FetchNextPageAsync(CancellationToken cancellationToken = default)
    {
        return await FetchNextPageAsync(cancellationToken, false).ConfigureAwait(false);
    }
    
    private async Task<FindPage<TResult>> FetchNextPageAsync(CancellationToken cancellationToken, bool runSynchronously)
    {
        if ((_buffer?.Count ?? 0) > 0)
        {
            throw new CursorException("Cannot fetch next page when the current page (the buffer) is not empty", State);
        }
        
        if (State == CursorState.Closed)
        {
            throw new CursorException("Cannot fetch next page on a closed cursor", State);
        }

        await MoveNextAsync(cancellationToken, peek: true, runSynchronously).ConfigureAwait(false);

        var buffer = _currentPage.Results;
        _currentPage.Results = new List<TResult>();
        
        return new FindPage<TResult>(_currentPage.NextPageState, buffer, _currentPage.SortVector);
    }
    
    /// <summary>
    /// Synchronously retrieves the sort vector used for the query.
    /// </summary>
    /// <returns>The sort vector, or null if not available.</returns>
    /// <remarks>
    /// The cursor must have been started (at least one record fetched) and <see cref="IncludeSortVector"/> must be enabled.
    /// The asynchronous version <see cref="GetSortVectorAsync(CancellationToken)"/> is recommended to avoid potential deadlocks.
    /// </remarks>
    public float[] GetSortVector()
    {
        return GetSortVectorAsync(CancellationToken.None, true).ResultSync();
    }
    
    /// <summary>
    /// Asynchronously retrieves the sort vector used for the query.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to cancel the operation.</param>
    /// <returns>The sort vector, or null if not available.</returns>
    /// <remarks>
    /// The cursor must have been started (at least one record fetched) and <see cref="IncludeSortVector"/> must be enabled.
    /// If the cursor hasn't been started yet, this method will automatically fetch the first page to retrieve the sort vector.
    /// </remarks>
    public async Task<float[]> GetSortVectorAsync(CancellationToken cancellationToken = default)
    {
        return await GetSortVectorAsync(cancellationToken, false);
    }
    
    private async Task<float[]> GetSortVectorAsync(CancellationToken cancellationToken, bool runSynchronously)
    {
        if (_currentPage == null && FindOptions.IncludeSortVector == true)
        {
            await MoveNextAsync(cancellationToken, peek: true, runSynchronously).ConfigureAwait(false);
        }
        return _currentPage?.SortVector;
    }
    
    /// <summary>
    /// Resets the cursor to its initial state, clearing the current page.
    /// </summary>
    public override void Rewind()
    {
        base.Rewind();
        _currentPage = null;
    }
    
    /// <summary>
    /// Releases all resources used by the cursor and clears the current page.
    /// </summary>
    public override void Dispose()
    {
        base.Dispose();
        _currentPage = null;
    }
    
    /// <summary>
    /// Fetches the next page of results from the underlying data source.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <param name="runSynchronously">Whether to run the operation synchronously.</param>
    /// <returns>True if more pages are available, false otherwise.</returns>
    protected override async Task<bool> FetchMoreAsync(CancellationToken cancellationToken, bool runSynchronously)
    {
        if (cancellationToken != CancellationToken.None)
        {
            FindOptions.BulkOperationCancellationToken = cancellationToken;
        }

        _currentPage = await FetchPageFunc((TCursor)this, _currentPage?.NextPageState, runSynchronously).ConfigureAwait(false);
        
        return _currentPage.NextPageState != null;
    }

    private TCursor UpdateOptions(Action<BaseFindManyOptions<T, TSort>> optionsUpdater)
    {
        if (State != CursorState.Idle)
        {
            throw new CursorException("Cursors must be idle when building their options", State);
        }
        var newOptions = FindOptions.Clone();
        optionsUpdater(newOptions);
        return CloneWith(CurrentFilter, newOptions);
    }
    
    /// <summary>
    /// Creates a new cursor instance with updated filter and options.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <returns>A new cursor instance.</returns>
    internal abstract TCursor CloneWith(Filter<T> filter, BaseFindManyOptions<T, TSort> options);
}
