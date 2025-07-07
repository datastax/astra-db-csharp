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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// A cursor for iterating over the results of a query in a streaming manner.
///  
/// When multiple results are returned by the underlying API, they are returned in batches.
/// You can use the <see cref="MoveNextAsync"/> method to iterate over the batches
/// and <see cref="Current"/> to access the current batch of results.
/// 
/// The <see cref="ToAsyncEnumerator"/> and <see cref="ToEnumerable"/> methods create a new cursor to ensure
/// iteration starts from the first batch, allowing multiple enumerations of the same query.
/// Use the cursor directly if you need access to the SortVector or want to manually control iterating over the batches.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class Cursor<T> : IDisposable, IParentCursor
{
    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
    private ApiFindResult<T> _currentBatch;
    private readonly Func<string, bool, Task<ApiResponseWithData<ApiFindResult<T>, FindStatusResult>>> _fetchNextBatch;
    private readonly IParentCursor _parentCursor;


    internal Cursor(
        Func<string, bool, Task<ApiResponseWithData<ApiFindResult<T>, FindStatusResult>>> fetchNextBatch,
        IParentCursor parentCursor = null)
    {
        _fetchNextBatch = fetchNextBatch ?? throw new ArgumentNullException(nameof(fetchNextBatch));
        _parentCursor = parentCursor;
    }

    /// <summary>
    /// Gets a value indicating whether the cursor has been started or not.
    /// </summary>
    public bool IsStarted { get; internal set; } = false;

    /// <summary>
    /// The current batch of results.
    /// </summary>
    public IEnumerable<T> Current
    {
        get
        {
            if (_currentBatch == null)
            {
                throw new InvalidOperationException("Cursor has not been started. Call MoveNextAsync first.");
            }
            return _currentBatch.Items;
        }
    }

    /// <summary>
    /// An array containing the sort vectors used for the query that created this cursor.
    /// </summary>
    public float[] SortVector { get; private set; }

    /// <summary>
    /// Synchronously moves the cursor to the next batch of results.
    /// </summary>
    /// <returns>True if there are more batches, false otherwise.</returns>
    /// <remarks>
    /// The asynchronous version <see cref="MoveNextAsync"/> is recommended to avoid potential deadlocks.
    /// </remarks>
    public bool MoveNext()
    {
        _semaphore.Wait();
        try
        {
            return MoveNextAsync(true).GetAwaiter().GetResult();
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// Asynchronously moves the cursor to the next batch of results.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to cancel the operation.</param>
    /// <returns>True if there are more batches, false otherwise.</returns>
    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await MoveNextAsync(false, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task<bool> MoveNextAsync(bool runSynchronously, CancellationToken cancellationToken = default)
    {
        if (_currentBatch != null && string.IsNullOrEmpty(_currentBatch.NextPageState))
        {
            return false;
        }

        _parentCursor?.SetStarted();

        var nextPageState = _currentBatch?.NextPageState;
        var nextBatch = await _fetchNextBatch(nextPageState, runSynchronously).ConfigureAwait(false);
        if (nextBatch.Data == null || nextBatch.Data.Items == null || nextBatch.Data.Items.Count == 0)
        {
            return false;
        }

        var nextSortVector = nextBatch.Status?.SortVector;
        _parentCursor?.SetSortVector(nextSortVector);

        _currentBatch = nextBatch.Data;
        return true;
    }

    public void Dispose()
    {
        _semaphore.Dispose();
    }

    /// <summary>
    /// Converts the cursor to an IAsyncEnumerator, starting from the first batch.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>An IAsyncEnumerator containing all documents in the cursor.</returns>
    public async IAsyncEnumerator<T> ToAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        using var newCursor = new Cursor<T>(_fetchNextBatch, this);
        while (await newCursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var item in newCursor.Current)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    /// <summary>
    /// Converts the cursor to an IEnumerable, starting from the first batch.
    /// </summary>
    /// <returns>An IEnumerable containing all documents in the cursor.</returns>
    public IEnumerable<T> ToEnumerable()
    {
        using var newCursor = new Cursor<T>(_fetchNextBatch, this);
        while (newCursor.MoveNext())
        {
            foreach (var item in newCursor.Current)
            {
                yield return item;
            }
        }
    }

    void IParentCursor.SetSortVector(float[] sortVector)
    {
        if ((SortVector == null || SortVector.Length == 0) && sortVector != null && sortVector.Length > 0)
        {
            SortVector = sortVector;
        }
    }

    void IParentCursor.SetStarted()
    {
        IsStarted = true;
    }

}
