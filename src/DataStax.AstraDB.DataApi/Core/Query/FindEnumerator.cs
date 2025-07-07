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

using DataStax.AstraDB.DataApi.Core.Results;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A Fluent API for finding and enumerating documents or rows.
/// </summary>
/// <typeparam name="T">The type representing the document or row.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (i.e. if using <see cref="Projection"/>).</typeparam>
public class FindEnumerator<T, TResult, TSort> : IAsyncEnumerable<TResult>, IEnumerable<TResult>
    where T : class
    where TResult : class
    where TSort : SortBuilder<T>
{
    private readonly IQueryRunner<T, TSort> _queryRunner;
    private readonly IFindManyOptions<T, TSort> _findOptions;
    private readonly CommandOptions _commandOptions;
    private Cursor<TResult> _cursor;

    internal FindEnumerator(IQueryRunner<T, TSort> queryRunner, IFindManyOptions<T, TSort> findOptions, CommandOptions commandOptions)
    {
        _queryRunner = queryRunner;
        _findOptions = findOptions.Clone();
        _commandOptions = commandOptions;
    }

    /// <summary>
    /// Specify a Projection to apply to the results of the operation.
    /// </summary>
    /// <param name="projection">The projection to apply.</param>
    /// <returns>The FindEnumerator instance to continue specifying the find options.</returns>
    /// <example>
    /// <code>
    /// // Inclusive Projection, return only the nested Properties.PropertyOne field
    /// var projectionBuilder = Builders&lt;SimpleObject&gt;.Projection;
    /// var projection = projectionBuilder.Include(p =&gt; p.Properties.PropertyOne);
    /// </code>
    /// </example>
    public FindEnumerator<T, TResult, TSort> Project(IProjectionBuilder projection)
    {
        return UpdateOptions(options => options.Projection = projection);
    }

    /// <summary>
    /// Specify the maximum number of documents to return.
    /// </summary>
    /// <param name="limit">The maximum number of documents to return.</param>
    /// <returns>The FindEnumerator instance to continue specifying the find options.</returns>
    public FindEnumerator<T, TResult, TSort> Limit(int limit)
    {
        return UpdateOptions(options => options.Limit = limit);
    }

    /// <summary>
    /// The number of documents to skip before starting to return documents.
    /// Use in conjuction with <see cref="Sort"/> to determine the order to apply before skipping. 
    /// </summary>
    /// <param name="skip">The number of documents to skip.</param>
    /// <returns>The FindEnumerator instance to continue specifying the find options.</returns>
    public FindEnumerator<T, TResult, TSort> Skip(int skip)
    {
        return UpdateOptions(options => options.Skip = skip);
    }

    /// <summary>
    /// Specify a Sort to use when running the find.
    /// </summary>
    /// <param name="sortBuilder">The sort to apply.</param>
    /// <returns>The FindEnumerator instance to continue adding options.</returns>
    /// <example>
    /// <code>
    /// // Sort by the nested Properties.PropertyOne field
    /// var sortBuilder = Builders&lt;SimpleObject&gt;.Sort;
    /// var sort = sortBuilder.Ascending(p =&gt; p.Properties.PropertyOne);
    /// </code>
    /// </example>
    public FindEnumerator<T, TResult, TSort> Sort(TSort sortBuilder)
    {
        return UpdateOptions(options => options.Sort = sortBuilder);
    }

    /// <summary>
    /// Whether to include the similarity score in the result or not.
    /// </summary>
    /// <param name="includeSimilarity">Whether to include the similarity score in the result or not.</param>
    /// <returns>The FindEnumerator instance to continue specifying the find options.</returns>
    /// <example>
    /// You can use the attribute <see cref="SerDes.DocumentMappingAttribute"/> to map the similarity score to the result class.
    /// <code>
    /// public class SimpleObjectWithVectorizeResult : SimpleObjectWithVectorize
    /// {
    ///     [DocumentMapping(DocumentMappingField.Similarity)]
    ///     public double? Similarity { get; set; }
    /// }
    /// 
    /// var FindEnumerator = collection.Find&lt;SimpleObjectWithVectorizeResult&gt;()
    ///     .Sort(Builders&lt;SimpleObjectWithVectorize&gt;.Sort.Vectorize(dogQueryVectorString))
    ///     .IncludeSimilarity(true);
    /// var cursor = FindEnumerator.ToCursor();
    /// var list = cursor.ToList();
    /// var result = list.First();
    /// var similarity = result.Similarity;
    /// </code>
    /// </example>
    public FindEnumerator<T, TResult, TSort> IncludeSimilarity(bool includeSimilarity)
    {
        return UpdateOptions(options => options.IncludeSimilarity = includeSimilarity);
    }

    /// <summary>
    /// Whether to include the sort vector in the result or not.
    /// </summary>
    /// <param name="includeSortVector">Whether to include the sort vector in the result or not.</param>
    /// <returns>The FindEnumerator instance to continue specifying the find options.</returns>
    /// <example>
    /// <code>
    /// var finder = collection.Find&lt;SimpleObjectWithVectorizeResult&gt;()
    ///     .Sort(Builders&lt;SimpleObjectWithVectorize&gt;.Sort.Vectorize(dogQueryVectorString))
    ///     .IncludeSortVector(true);
    /// //enumerate the results
    /// var results = await finder.ToList();
    /// var sortVector = finder.GetSortVector();
    /// </code>
    /// </example>
    public FindEnumerator<T, TResult, TSort> IncludeSortVector(bool includeSortVector)
    {
        return UpdateOptions(options => options.IncludeSortVector = includeSortVector);
    }

    /// <summary>
    /// Returns the sort vector created by the vectorize sort.
    /// </summary>
    /// <returns>The sort vector.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the enumerator has not been started.</exception>
    public float[] GetSortVector()
    {
        var cursor = ToCursor();
        if (!cursor.IsStarted)
        {
            throw new InvalidOperationException("Enumerator has not been started. Enumerate the results first, call ToList(), or manually manage paging by calling ToCursor() and using MoveNextAsync() to iterate over the results.");
        }
        return cursor.SortVector;
    }

    /// <summary>
    /// Returns a cursor to iterate over the results of the find operation page by page.
    /// 
    /// NOTE: It is recommended to use the find results as an IEnumerable or IAsyncEnumerable instead of using the Cursor directly.
    /// The only situation where you would need to use the Cursor is when you need to access the sort vectors (see <see cref="IncludeSortVector(bool)"/>)
    /// </summary>
    /// <returns>A cursor to iterate over the results of the find operation page by page.</returns>
    public Cursor<TResult> ToCursor()
    {
        if (_cursor != null)
        {
            return _cursor;
        }
        _cursor = new Cursor<TResult>((string pageState, bool runSynchronously) => RunAsync(pageState, runSynchronously));
        return _cursor;
    }

    /// <summary>
    /// Returns an async enumerator to iterate over the results of the find operation.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to use for the operation.</param>
    /// <returns>An async enumerator</returns>
    public IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var cursor = ToCursor();
        return cursor.ToAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    /// Returns an enumerator to iterate over the results of the find operation.    
    /// </summary>
    /// <returns>An enumerator</returns>
    public IEnumerator<TResult> GetEnumerator()
    {
        var cursor = ToCursor();
        return cursor.ToEnumerable().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    private Task<ApiResponseWithData<ApiFindResult<TResult>, FindStatusResult>> RunAsync(string pageState = null, bool runSynchronously = false)
    {
        _findOptions.PageState = pageState;
        return _queryRunner.RunFindManyAsync<TResult>(_findOptions.Filter, _findOptions, _commandOptions, runSynchronously);
    }

    private FindEnumerator<T, TResult, TSort> UpdateOptions(Action<IFindManyOptions<T, TSort>> optionsUpdater)
    {
        optionsUpdater(_findOptions);
        return new FindEnumerator<T, TResult, TSort>(_queryRunner, _findOptions, _commandOptions);
    }

}