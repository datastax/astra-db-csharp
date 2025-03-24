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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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
            //TODO: handle states, for example: throw exception if MoveNext() returned false (i.e. no more documents)
            return _currentBatch.Documents;
        }
    }

    public float[] SortVector { get; internal set; } = Array.Empty<float>();

    internal Cursor(ApiResponseWithData<DocumentsResult<T>, FindStatusResult> currentResult, Func<string, bool, Task<ApiResponseWithData<DocumentsResult<T>, FindStatusResult>>> fetchNextBatch)
    {
        _currentBatch = currentResult.Data;
        if (_currentBatch != null && currentResult.Status != null)
        {
            SortVector = currentResult.Status.SortVector;
        }
        FetchNextBatch = fetchNextBatch;
    }

    public bool MoveNext()
    {
        if (string.IsNullOrEmpty(_currentBatch.NextPageState))
        {
            return false;
        }
        var nextResult = FetchNextBatch(_currentBatch.NextPageState, true).ResultSync();
        _currentBatch = nextResult.Data;
        if (nextResult.Status != null && nextResult.Status.SortVector != null)
        {
            SortVector = SortVector.Concat(nextResult.Status.SortVector).ToArray();
        }
        return true;
    }

    public async Task<bool> MoveNextAsync()
    {
        if (string.IsNullOrEmpty(_currentBatch.NextPageState))
        {
            return false;
        }
        var nextResult = await FetchNextBatch(_currentBatch.NextPageState, false).ConfigureAwait(false);
        _currentBatch = nextResult.Data;
        if (nextResult.Status != null && nextResult.Status.SortVector != null)
        {
            SortVector = SortVector.Concat(nextResult.Status.SortVector).ToArray();
        }
        return true;
    }
}

public static class CursorExtensions
{
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this Cursor<T> cursor) where T : class
    {
        bool hasNext;
        do
        {
            var currentItems = cursor.Current;
            if (currentItems == null)
            {
                yield break;
            }

            hasNext = await cursor.MoveNextAsync().ConfigureAwait(false);

            foreach (var item in currentItems)
            {
                yield return item;
            }
        } while (hasNext);
    }

    public static IEnumerable<T> ToEnumerable<T>(this Cursor<T> cursor) where T : class
    {
        return new CursorEnumerable<T>(cursor);
    }

    public static List<T> ToList<T>(this Cursor<T> cursor) where T : class
    {
        return new CursorEnumerable<T>(cursor).ToList();
    }
}

public class CursorEnumerable<T> : IEnumerable<T> where T : class
{
    private Cursor<T> _cursor;
    public CursorEnumerable(Cursor<T> cursor)
    {
        _cursor = cursor;
    }

    public IEnumerator<T> GetEnumerator()
    {
        return new CursorEnumerator<T>(_cursor);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

public class CursorEnumerator<T> : IEnumerator<T>
{
    private Cursor<T> _cursor;
    private List<T> _currentPageItems;
    private int _currentIndex = -1;
    private bool _isFinished = false;

    public CursorEnumerator(Cursor<T> cursor)
    {
        _cursor = cursor;
        _currentPageItems = cursor.Current.ToList();
    }

    public T Current
    {
        get
        {
            if (_currentPageItems == null || _currentIndex < 0 || _currentIndex >= _currentPageItems.Count)
            {
                throw new InvalidOperationException("Current is not valid before MoveNext or after enumeration is finished.");
            }
            return _currentPageItems[_currentIndex];
        }
    }

    object IEnumerator.Current => Current;

    public void Dispose()
    {
        _cursor = null;
    }

    public bool MoveNext()
    {
        if (_isFinished)
        {
            return false;
        }

        _currentIndex++;

        if (_currentPageItems == null || _currentIndex >= _currentPageItems.Count)
        {
            return FetchNextPage();
        }

        return true;
    }

    private bool FetchNextPage()
    {
        var hasNext = _cursor.MoveNext();
        if (!hasNext)
        {
            _isFinished = true;
            return false;
        }

        _currentPageItems = _cursor.Current.ToList();
        _currentIndex = 0;

        if (_currentPageItems == null || _currentPageItems.Count == 0)
        {
            _isFinished = true;
            return false;
        }

        return true;
    }

    public void Reset()
    {
        _currentIndex = -1;
        _currentPageItems = new List<T>();
        _isFinished = false;
    }

}