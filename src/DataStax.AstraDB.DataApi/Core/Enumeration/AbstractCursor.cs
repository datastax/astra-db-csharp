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

using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Enumeration;

/// <summary>
/// Represents the current state of a cursor.
/// </summary>
public enum CursorState
{
    /// <summary>
    /// The cursor has not been started yet.
    /// </summary>
    Idle,
    /// <summary>
    /// The cursor has been started and is actively iterating.
    /// </summary>
    Started,
    /// <summary>
    /// The cursor has been closed and can no longer be used.
    /// </summary>
    Closed
}

/// <summary>
/// Exception thrown when a cursor operation is attempted in an invalid state.
/// </summary>
public class CursorException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CursorException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="state">The cursor state when the error occurred.</param>
    public CursorException(string message, CursorState state) : base(message)
    {
        CursorState = state;
    }

    /// <summary>
    /// Gets the cursor state when the error occurred.
    /// </summary>
    public CursorState CursorState { get; }
}

/// <summary>
/// Abstract base class for cursors that iterate over query results in a streaming manner.
/// 
/// This cursor provides state management, buffering, and both synchronous and asynchronous enumeration
/// capabilities. Results are fetched in batches from the underlying API and buffered for efficient iteration.
/// </summary>
/// <typeparam name="T">The type of the items or rows in the cursor.</typeparam>
/// <typeparam name="TCursor">The type of the cursor.</typeparam>
public abstract class AbstractCursor<T, TCursor> : IDisposable, IEnumerable<T>, IAsyncEnumerable<T>
    where TCursor : AbstractCursor<T, TCursor>
{
    /// <summary>
    /// Gets the internal buffer containing fetched results.
    /// </summary>
    protected abstract List<T> _buffer { get; }
    private bool _isNextPage = true;
    
    /// <summary>
    /// Gets the current state of the cursor.
    /// </summary>
    public CursorState State { get; protected set; } = CursorState.Idle;

    /// <summary>
    /// Returns the number of items currently buffered in memory.
    /// </summary>
    /// <returns>The count of buffered items.</returns>
    public int Buffered() => _buffer?.Count ?? 0;
    
    /// <summary>
    /// Gets the total number of items consumed from the cursor so far.
    /// </summary>
    public int Consumed { get; protected set; }

    /// <summary>
    /// Consumes and returns items from the buffer.
    /// </summary>
    /// <param name="max">The maximum number of items to consume. If 0 or omitted, consumes all buffered items.</param>
    /// <returns>A read-only list of consumed items.</returns>
    public IReadOnlyList<T> ConsumeBuffer(int max = 0)
    {
        if (_buffer == null || _buffer.Count == 0)
        {
            return new List<T>();
        }

        var count = max > 0 ? Math.Min(max, _buffer.Count) : _buffer.Count;
        var result = _buffer.Take(count).ToList();
        _buffer.RemoveRange(0, count);
        Consumed += count;

        return result;
    }
    
    /// <summary>
    /// Synchronously checks if there are more items available without consuming them.
    /// </summary>
    /// <returns>True if there are more items available, false otherwise.</returns>
    /// <remarks>
    /// The asynchronous version <see cref="HasNextAsync(CancellationToken)"/> is recommended to avoid potential deadlocks.
    /// </remarks>
    public bool HasNext()
    {
        return MoveNextAsync(CancellationToken.None, peek: true, runSynchronously: true).ResultSync() != null;
    }

    /// <summary>
    /// Asynchronously checks if there are more items available without consuming them.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to cancel the operation.</param>
    /// <returns>True if there are more items available, false otherwise.</returns>
    public async Task<bool> HasNextAsync(CancellationToken cancellationToken = default)
    {
        return await MoveNextAsync(cancellationToken, peek: true, runSynchronously: false) != null;
    }

    /// <summary>
    /// Synchronously moves to and returns the next item in the cursor.
    /// </summary>
    /// <returns>The next item, or null if there are no more items.</returns>
    /// <remarks>
    /// The asynchronous version <see cref="MoveNextAsync(CancellationToken)"/> is recommended to avoid potential deadlocks.
    /// </remarks>
    public T MoveNext()
    {
        return MoveNextAsync(CancellationToken.None, peek: false, runSynchronously: true).ResultSync();
    }
    
    /// <summary>
    /// Asynchronously moves to and returns the next item in the cursor.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to cancel the operation.</param>
    /// <returns>The next item, or null if there are no more items.</returns>
    public async Task<T> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        return await MoveNextAsync(cancellationToken, peek: false, runSynchronously: false);
    }

    /// <summary>
    /// Returns an async enumerator to iterate over all items in the cursor.
    /// </summary>
    /// <param name="cancellationToken">An optional cancellation token to cancel the operation.</param>
    /// <returns>An async enumerator for the cursor.</returns>
    /// <exception cref="CursorException">Thrown when attempting to iterate over a closed cursor.</exception>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (State == CursorState.Closed)
        {
            throw new CursorException("Cannot iterate over a closed cursor", State);
        }
        
        T doc;
        while ((doc = await MoveNextAsync(cancellationToken)) != null)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return doc;
        }
    }

    /// <summary>
    /// Returns an enumerator to iterate over all items in the cursor.
    /// </summary>
    /// <returns>An enumerator for the cursor.</returns>
    /// <exception cref="CursorException">Thrown when attempting to iterate over a closed cursor.</exception>
    public IEnumerator<T> GetEnumerator()
    {
        if (State == CursorState.Closed)
        {
            throw new CursorException("Cannot iterate over a closed cursor", State);
        }
        
        T doc;
        while ((doc = MoveNext()) != null)
        {
            yield return doc;
        }
    }

    /// <summary>
    /// Returns an enumerator to iterate over all items in the cursor.
    /// </summary>
    /// <returns>An enumerator for the cursor.</returns>
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    /// Creates a new cursor instance with the same configuration.
    /// </summary>
    /// <returns>A new cursor instance.</returns>
    public abstract TCursor Clone();

    /// <summary>
    /// Releases all resources used by the cursor and sets its state to <see cref="CursorState.Closed"/>.
    /// </summary>
    public virtual void Dispose()
    {
        State = CursorState.Closed;
        _isNextPage = false;
        _buffer?.Clear();
    }

    /// <summary>
    /// Resets the cursor to its initial state, allowing iteration to start over from the beginning.
    /// </summary>
    /// <remarks>
    /// This resets the consumed count and cursor state, but does not refetch data.
    /// The next iteration will start from the first page again.
    /// </remarks>
    public virtual void Rewind()
    {
        Consumed = 0;
        State = CursorState.Idle;
        _isNextPage = true;
    }

    /// <summary>
    /// Fetches the next page of results from the underlying data source.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <param name="runSynchronously">Whether to run the operation synchronously.</param>
    /// <returns>True if more pages are available, false otherwise.</returns>
    protected abstract Task<bool> FetchNextPageAsync(CancellationToken cancellationToken, bool runSynchronously);
    
    /// <summary>
    /// Internal method to move to the next item, with options to peek or consume.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <param name="peek">If true, returns the next item without consuming it.</param>
    /// <param name="runSynchronously">Whether to run the operation synchronously.</param>
    /// <returns>The next item, or null if there are no more items.</returns>
    protected async Task<T> MoveNextAsync(CancellationToken cancellationToken, bool peek, bool runSynchronously)
    {
        if (State == CursorState.Closed)
        {
            return default;
        }

        State = CursorState.Started;

        while (_buffer == null || _buffer.Count == 0)
        {
            if (!_isNextPage)
            {
                State = CursorState.Closed;
                return default;
            }

            _isNextPage = await FetchNextPageAsync(cancellationToken, runSynchronously).ConfigureAwait(false);
        }

        if (peek)
        {
            return _buffer.FirstOrDefault();
        }

        return ConsumeBuffer(1).FirstOrDefault();
    }

}
