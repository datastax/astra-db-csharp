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

using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core.Results;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class FluentFind<T, TId, TResult> : IAsyncEnumerable<TResult>, IEnumerable<TResult>
    where T : class
    where TResult : class
{
    private readonly Filter<T> _filter;
    private readonly Collection<T, TId> _collection;
    private FindOptions<T> _findOptions;
    private CommandOptions _commandOptions;

    internal Filter<T> Filter => _filter;
    internal Collection<T, TId> Collection => _collection;

    private FindOptions<T> FindOptions
    {
        get { return _findOptions ??= new FindOptions<T>(); }
    }

    internal FluentFind(Collection<T, TId> collection, Filter<T> filter, FindOptions<T> findOptions, CommandOptions commandOptions)
    {
        _filter = filter;
        _collection = collection;
        _findOptions = findOptions;
        _commandOptions = commandOptions;
    }

    public FluentFind<T, TId, TResult> Project(IProjectionBuilder projection)
    {
        FindOptions.Projection = projection;
        return this;
    }

    public FluentFind<T, TId, TResult> Sort(SortBuilder<T> sortBuilder)
    {
        FindOptions.Sort = sortBuilder;
        return this;
    }

    public FluentFind<T, TId, TResult> Limit(int limit)
    {
        FindOptions.Limit = limit;
        return this;
    }

    public FluentFind<T, TId, TResult> Skip(int skip)
    {
        FindOptions.Skip = skip;
        return this;
    }

    public FluentFind<T, TId, TResult> IncludeSimilarity(bool includeSimilarity)
    {
        FindOptions.IncludeSimilarity = includeSimilarity;
        return this;
    }

    public FluentFind<T, TId, TResult> IncludeSortVector(bool includeSortVector)
    {
        FindOptions.IncludeSortVector = includeSortVector;
        return this;
    }

    internal Task<ApiResponseWithData<DocumentsResult<TResult>, FindStatusResult>> RunAsync(string pageState = null, bool runSynchronously = false)
    {
        FindOptions.PageState = pageState;
        return _collection.RunFindManyAsync<TResult>(_filter, FindOptions, _commandOptions, runSynchronously);
    }

    public Cursor<TResult> ToCursor()
    {
        var cursor = new Cursor<TResult>((string pageState, bool runSynchronously) => RunAsync(pageState, runSynchronously));
        return cursor;
    }

    public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var cursor = ToCursor();
        await foreach (var item in cursor.ToAsyncEnumerable(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public IEnumerator<TResult> GetEnumerator()
    {
        var cursor = ToCursor();
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

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

}