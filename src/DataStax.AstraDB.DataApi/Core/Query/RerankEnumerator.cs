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

using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A Fluent API for finding documents in a collection.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
/// <typeparam name="TResult">The type of the result.</typeparam>
public class RerankEnumerator<T, TResult> : IAsyncEnumerable<TResult>, IEnumerable<TResult>
    where T : class
    where TResult : class
{
    private readonly Func<Command> _commandFactory;
    private readonly CommandOptions _commandOptions;
    private readonly FindAndRerankOptions<T> _findOptions;

    private volatile Task<ApiResponseWithData<ApiFindResult<TResult>, FindStatusResult<RerankedResult<TResult>>>> _resultTask;

    internal RerankEnumerator(Func<Command> commandFactory, FindAndRerankOptions<T> findOptions, CommandOptions commandOptions)
    {
        _commandFactory = commandFactory;
        _commandOptions = commandOptions;
        _findOptions = findOptions.Clone();
    }

    /// <summary>
    /// Set the field to rerank on (defaults to the lexical sort parameter)
    /// </summary>
    /// <param name="rerankOn"></param>
    /// <returns></returns>
    public RerankEnumerator<T, TResult> SetRerankOn(string rerankOn)
    {
        _findOptions.RerankOn = rerankOn;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Set the field to rerank on (defaults to the lexical sort parameter)
    /// </summary>
    /// <param name="rerankOn"></param>
    /// <returns></returns>
    public RerankEnumerator<T, TResult> SetRerankOn<TField>(Expression<Func<T, TField>> rerankOn)
    {
        _findOptions.RerankOn = rerankOn.GetMemberNameTree();
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Whether to include the scores (vector similarity and reranker) in the result or not.
    /// </summary>
    /// <param name="includeScores">Whether to include the scores in the result or not.</param>
    /// <returns></returns>
    /// <example>
    /// <code>
    /// var findAndReranker = collection.Find<SimpleObjectWithVectorizeResult>()
    ///     .Sort(Builders<SimpleObjectWithVectorize>.Sort.Vectorize(dogQueryVectorString))
    ///     .IncludeScores(true);
    /// var documentsWithScores = findAndReranker.WithScoresAsync();
    /// await foreach (var document in documentsWithScores)
    /// {
    ///     Console.WriteLine(document.Document);
    ///     Console.WriteLine(document.Scores);
    /// }
    /// </code>
    /// </example>
    public RerankEnumerator<T, TResult> IncludeScores(bool includeScores)
    {
        _findOptions.IncludeScores = includeScores;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Whether to include the sort vector in the result or not.
    /// </summary>
    /// <param name="includeSortVector">Whether to include the sort vector in the result or not.</param>
    /// <returns></returns>
    /// <example>
    /// To access the sort vectors, you need to use <see cref="Cursor{T}.SortVector"/> after calling <see cref="ToCursor()"/> on your FindAndReranker instance.
    /// <code>
    /// var FindAndReranker = collection.Find<SimpleObjectWithVectorizeResult>()
    ///     .Sort(Builders<SimpleObjectWithVectorize>.Sort.Vectorize(dogQueryVectorString))
    ///     .IncludeSortVector(true);
    /// var cursor = FindAndReranker.ToCursor();
    /// var sortVector = cursor.SortVector;
    /// </code>
    /// </example>
    public RerankEnumerator<T, TResult> IncludeSortVector(bool includeSortVector)
    {
        _findOptions.IncludeSortVector = includeSortVector;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Set the search string for the reranker to use. This must be set if you pass a vector to the Sort method.
    /// If you use the hybrid sort, or pass a string to be vectorized to the Sort method, that string will
    /// be used unless you pass a different string to this method.
    /// </summary>
    /// <param name="rerankQuery">The rerank query to use.</param>
    /// <returns></returns>
    public RerankEnumerator<T, TResult> SetRerankQuery(string rerankQuery)
    {
        _findOptions.RerankQuery = rerankQuery;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Set the limit for the number of documents to return.
    /// </summary>
    /// <param name="limit">The number of documents to return.</param>
    /// <returns></returns>
    public RerankEnumerator<T, TResult> Limit(int limit)
    {
        _findOptions.Limit = limit;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Customize the number of documents to return for each of the underlying searches (vector and lexical).
    /// </summary>
    /// <param name="lexicalLimit"></param>
    /// <param name="vectorLimit"></param>
    /// <returns></returns>
    public RerankEnumerator<T, TResult> SetHybridLimits(int? lexicalLimit, int? vectorLimit)
    {
        Dictionary<string, int> hybridLimits = new();
        if (lexicalLimit != null)
        {
            hybridLimits.Add(DataApiKeywords.Lexical, lexicalLimit.Value);
        }
        if (vectorLimit != null)
        {
            hybridLimits.Add(DataApiKeywords.Vector, vectorLimit.Value);
        }
        _findOptions.HybridLimits = hybridLimits;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Specify a Projection to apply to the results of the operation.
    /// </summary>
    /// <param name="projection">The projection to apply.</param>
    /// <returns></returns>
    /// <example>
    /// <code>
    /// // Inclusive Projection, return only the nested Properties.PropertyOne field
    /// var projectionBuilder = Builders<SimpleObject>.Projection;
    /// var projection = projectionBuilder.Include(p => p.Properties.PropertyOne);
    /// </code>
    /// </example>
    public RerankEnumerator<T, TResult> Project(IProjectionBuilder projection)
    {
        _findOptions.Projection = projection;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Get the sort vector for the find operation.
    /// </summary>
    /// <returns></returns>
    /// <remarks>
    /// The sort vector is only available if <see cref="IncludeSortVector(bool)"/> was called on the FindAndReranker instance.
    /// This method will cause the find operation to run if it has not already been run.
    /// </remarks>
    public async Task<float[]> GetSortVectorAsync()
    {
        if (!_findOptions.IncludeSortVector == true)
        {
            throw new InvalidOperationException("IncludeSortVector must be set to true before calling GetSortVector");
        }
        var response = await GetAsync().ConfigureAwait(false);
        return response.Status.SortVector;
    }

    /// <summary>
    /// Synchronous version of <see cref="GetSortVectorAsync()"/>
    /// </summary>
    /// <inheritdoc cref="GetSortVectorAsync()"/>
    public float[] GetSortVector()
    {
        return GetSortVectorAsync().ResultSync();
    }

    /// <summary>
    /// Returns an async enumerator to iterate over the results of the find operation.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to use for the operation.</param>
    /// <returns>An async enumerator</returns>
    public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var response = await GetAsync().ConfigureAwait(false);

        foreach (var item in response.Data.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    /// <summary>
    /// Returns an enumerator to iterate over the results of the find operation.    
    /// </summary>
    /// <returns>An enumerator</returns>
    public IEnumerator<TResult> GetEnumerator()
    {
        var response = GetAsync(true).ResultSync();
        foreach (var item in response.Data.Items)
        {
            yield return item;
        }
    }

    /// <summary>
    /// Returns an async enumerator to iterate over the results of the find operation, including the scores.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to use for the operation.</param>
    /// <returns>An async enumerator of <see cref="RerankedResult{TResult}"/> objects which include the actual document and the scores.</returns>
    public async IAsyncEnumerable<RerankedResult<TResult>> WithScoresAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_findOptions.IncludeScores != true)
        {
            throw new InvalidOperationException("IncludeScores must be set to true before calling WithScores");
        }
        var response = await GetAsync().ConfigureAwait(false);
        foreach (var result in GetRerankedResults(response, cancellationToken))
        {
            yield return result;
        }
    }

    /// <summary>
    /// Returns an enumerator to iterate over the results of the find operation, including the scores.
    /// </summary>
    /// <returns>An enumerator of <see cref="RerankedResult{TResult}"/> objects which include the actual document and the scores.</returns>
    public IEnumerator<RerankedResult<TResult>> WithScores()
    {
        if (_findOptions.IncludeScores != true)
        {
            throw new InvalidOperationException("IncludeScores must be set to true before calling WithScores");
        }
        var response = GetAsync(true).ResultSync();
        foreach (var result in GetRerankedResults(response))
        {
            yield return result;
        }
    }

    internal async Task<ApiResponseWithData<ApiFindResult<TResult>, FindStatusResult<RerankedResult<TResult>>>> GetAsync(bool runSynchronously = false)
    {
        if (_resultTask == null)
        {
            var command = _commandFactory().WithPayload(_findOptions).AddCommandOptions(_commandOptions);
            _resultTask = command.RunAsyncReturnDocumentData<ApiFindResult<TResult>, TResult, FindStatusResult<RerankedResult<TResult>>>(runSynchronously);
        }

        return await _resultTask.ConfigureAwait(false);
    }

    private static IEnumerable<RerankedResult<TResult>> GetRerankedResults(ApiResponseWithData<ApiFindResult<TResult>, FindStatusResult<RerankedResult<TResult>>> response, CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < response.Data.Items.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var item = response.Data.Items[i];
            var scores = response.Status.DocumentResponses == null ? null : response.Status.DocumentResponses[i].Scores;
            yield return new RerankedResult<TResult> { Document = item, Scores = scores };
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

}
