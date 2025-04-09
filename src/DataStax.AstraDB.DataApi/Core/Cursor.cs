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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DataStax.AstraDB.DataApi.Core.Results;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// A cursor for iterating over the results of a query.
///  
/// When multiple results are returned by the underlying API, they are returned in batches.
/// You can use the <see cref="MoveNext()"/> or <see cref="MoveNextAsync()"/> methods to iterate over the batches
/// and <see cref="Current"/> to access the current batch of results.
/// 
/// In most situations, using the results of a query directly as an IEnumerable or IAsyncEnumerable is recommended.
/// Use the cursor directly if you need access to the SortVectors or want to manually control iterating over the batches.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class Cursor<T>
{
    private DocumentsResult<T> _currentBatch;
    private Func<string, bool, Task<ApiResponseWithData<DocumentsResult<T>, FindStatusResult>>> FetchNextBatch { get; }

    /// <summary>
    /// The current batch of results.
    /// </summary>
    public IEnumerable<T> Current
    {
        get
        {
            if (_currentBatch == null)
            {
                throw new Exception("Cursor has not been started. Please call MoveNext()");
            }
            return _currentBatch.Documents;
        }
    }

    /// <summary>
    /// An array containing the sort vectors used for this query.
    /// </summary>
    public float[] SortVectors { get; internal set; } = Array.Empty<float>();

    internal Cursor(Func<string, bool, Task<ApiResponseWithData<DocumentsResult<T>, FindStatusResult>>> fetchNextBatch)
    {
        FetchNextBatch = fetchNextBatch;
    }

    /// <summary>
    /// Synchronously moves the cursor to the next batch of results.
    /// </summary>
    /// <returns>True if there are more batches, false otherwise.</returns>
    /// <remarks>
    /// The asynchronous version <see cref="MoveNextAsync()"/> of this method is recommended.
    /// </remarks>
    public bool MoveNext()
    {
        ApiResponseWithData<DocumentsResult<T>, FindStatusResult> nextResult;
        if (_currentBatch == null)
        {
            nextResult = FetchNextBatch(null, true).ResultSync();
            if (nextResult.Data == null || nextResult.Data.Documents.Count == 0)
            {
                return false;
            }
        }
        else
        {
            if (string.IsNullOrEmpty(_currentBatch.NextPageState))
            {
                return false;
            }
            nextResult = FetchNextBatch(_currentBatch.NextPageState, true).ResultSync();
        }
        if (nextResult.Status != null && nextResult.Status.SortVector != null)
        {
            SortVectors = SortVectors.Concat(nextResult.Status.SortVector).ToArray();
        }
        _currentBatch = nextResult.Data;
        return true;
    }

    /// <summary>
    /// Moves the cursor to the next batch of results.
    /// </summary>
    /// <returns>True if there are more batches, false otherwise.</returns>
    public async Task<bool> MoveNextAsync()
    {
        ApiResponseWithData<DocumentsResult<T>, FindStatusResult> nextResult;
        if (_currentBatch == null)
        {
            nextResult = await FetchNextBatch(null, true).ConfigureAwait(false);
            if (nextResult.Data == null || nextResult.Data.Documents.Count == 0)
            {
                return false;
            }
        }
        else
        {
            if (string.IsNullOrEmpty(_currentBatch.NextPageState))
            {
                return false;
            }
            nextResult = await FetchNextBatch(_currentBatch.NextPageState, true).ConfigureAwait(false);
        }
        if (nextResult.Status != null && nextResult.Status.SortVector != null)
        {
            SortVectors = SortVectors.Concat(nextResult.Status.SortVector).ToArray();
        }
        _currentBatch = nextResult.Data;
        return true;
    }
}

/// <summary>
/// Extensions for <see cref="Cursor{T}"/>.
/// </summary>
public static class CursorExtensions
{
    /// <summary>
    /// Converts the cursor to an IAsyncEnumerable.
    /// </summary>
    /// <typeparam name="TResult">The type of the documents in the collection.</typeparam>
    /// <param name="cursor">The cursor to convert.</param>
    /// <param name="cancellationToken">An optional cancellation token.</param>
    /// <returns>An IAsyncEnumerable containing all of the documents in the cursor.</returns>
    public static async IAsyncEnumerable<TResult> ToAsyncEnumerable<TResult>(this Cursor<TResult> cursor, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        bool hasNext;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            hasNext = await cursor.MoveNextAsync().ConfigureAwait(false);
            if (!hasNext || cursor.Current == null)
            {
                yield break;
            }
            foreach (var item in cursor.Current)
            {
                yield return item;
            }
        } while (hasNext);
    }

    /// <summary>
    /// Converts the cursor to an IEnumerable.
    /// </summary>
    /// <typeparam name="TResult">The type of the documents in the collection.</typeparam>
    /// <param name="cursor">The cursor to convert.</param>
    /// <returns>An IEnumerable containing all of the documents in the cursor.</returns>
    public static IEnumerable<TResult> ToEnumerable<TResult>(this Cursor<TResult> cursor)
    {
        bool hasNext;
        do
        {
            hasNext = cursor.MoveNext();
            if (!hasNext || cursor.Current == null)
            {
                yield break;
            }
            foreach (var item in cursor.Current)
            {
                yield return item;
            }
        } while (hasNext);
    }

    /// <summary>
    /// Returns all of the results of the cursor as a List.
    /// </summary>
    /// <typeparam name="TResult">The type of the documents in the collection.</typeparam>
    /// <param name="cursor">The cursor to convert.</param>
    /// <returns>A List containing all of the documents in the cursor.</returns>
    public static List<TResult> ToList<TResult>(this Cursor<TResult> cursor)
    {
        return ToEnumerable(cursor).ToList();
    }
}