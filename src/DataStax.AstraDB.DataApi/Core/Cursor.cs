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

public class Cursor<T>
{
    private DocumentsResult<T> _currentBatch;
    private Func<string, bool, Task<ApiResponseWithData<DocumentsResult<T>, FindStatusResult>>> FetchNextBatch { get; }

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

    public float[] SortVectors { get; internal set; } = Array.Empty<float>();

    internal Cursor(Func<string, bool, Task<ApiResponseWithData<DocumentsResult<T>, FindStatusResult>>> fetchNextBatch)
    {
        FetchNextBatch = fetchNextBatch;
    }

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

public static class CursorExtensions
{
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

    public static List<TResult> ToList<TResult>(this Cursor<TResult> cursor)
    {
        return ToEnumerable(cursor).ToList();
    }
}