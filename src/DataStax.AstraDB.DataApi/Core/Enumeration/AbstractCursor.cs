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

public enum CursorState
{
    Idle,
    Started,
    Closed
}

public class CursorError : Exception
{
    public CursorError(string message, CursorState state) : base(message)
    {
        CursorState = state;
    }

    public CursorState CursorState { get; }
}

public abstract class AbstractCursor<T> : IDisposable, IEnumerable<T>, IAsyncEnumerable<T>
{
    protected abstract List<T> _buffer { get; }
    private bool _isNextPage = true;
    
    public CursorState State { get; protected set; } = CursorState.Idle;

    public int Buffered() => _buffer?.Count ?? 0;
    
    public int Consumed { get; protected set; }

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
    
    public bool HasNext()
    {
        return MoveNextAsync(CancellationToken.None, peek: true, runSynchronously: true).ResultSync() != null;
    }

    public async Task<bool> HasNextAsync(CancellationToken cancellationToken = default)
    {
        return await MoveNextAsync(cancellationToken, peek: true, runSynchronously: false) != null;
    }

    public T MoveNext()
    {
        return MoveNextAsync(CancellationToken.None, peek: false, runSynchronously: true).ResultSync();
    }
    
    public async Task<T> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        return await MoveNextAsync(cancellationToken, peek: false, runSynchronously: false);
    }

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (State == CursorState.Closed)
        {
            throw new CursorError("Cannot iterate over a closed cursor", State);
        }
        
        T doc;
        while ((doc = await MoveNextAsync(cancellationToken)) != null)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return doc;
        }
    }

    public IEnumerator<T> GetEnumerator()
    {
        if (State == CursorState.Closed)
        {
            throw new CursorError("Cannot iterate over a closed cursor", State);
        }
        
        T doc;
        while ((doc = MoveNext()) != null)
        {
            yield return doc;
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public abstract AbstractCursor<T> Clone();

    public virtual void Dispose()
    {
        State = CursorState.Closed;
        _isNextPage = false;
        _buffer?.Clear();
    }

    public virtual void Rewind()
    {
        Consumed = 0;
        State = CursorState.Idle;
        _isNextPage = true;
    }

    protected abstract Task<bool> FetchNextPageAsync(CancellationToken cancellationToken, bool runSynchronously);
    
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
