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
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class FluentFind<T, TId, TResult> where T : class where TResult : class
{
    private readonly Filter<T> _filter;
    private readonly Collection<T, TId> _collection;
    private FindOptions<T> _findOptions;

    internal Filter<T> Filter => _filter;
    internal Collection<T, TId> Collection => _collection;

    private FindOptions<T> FindOptions
    {
        get { return _findOptions ??= new FindOptions<T>(); }
    }

    internal FluentFind(Collection<T, TId> collection, Filter<T> filter)
    {
        _filter = filter;
        _collection = collection;
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
        return _collection.FindManyAsync<TResult>(_filter, FindOptions, null, runSynchronously);
    }

}

public static class FluentFindExtensions
{
    public static async Task<Cursor<TResult>> ToCursorAsync<T, TId, TResult>(this FluentFind<T, TId, TResult> fluentFind) where T : class where TResult : class
    {
        var initialResults = await fluentFind.RunAsync().ConfigureAwait(false);
        var cursor = new Cursor<TResult>(initialResults, (string pageState, bool runSynchronously) => fluentFind.RunAsync(pageState, runSynchronously));
        return cursor;
    }

    public static async IAsyncEnumerable<TResult> ToAsyncEnumerable<T, TId, TResult>(this FluentFind<T, TId, TResult> fluentFind) where T : class where TResult : class
    {
        var cursor = await fluentFind.ToCursorAsync().ConfigureAwait(false);
        await foreach (var item in cursor.ToAsyncEnumerable())
        {
            yield return item;
        }
    }

    public static async Task<IEnumerable<TResult>> ToEnumerableAsync<T, TId, TResult>(this FluentFind<T, TId, TResult> fluentFind) where T : class where TResult : class
    {
        var cursor = await fluentFind.ToCursorAsync().ConfigureAwait(false);
        return cursor.ToEnumerable();
    }

    public static async Task<List<TResult>> ToListAsync<T, TId, TResult>(this FluentFind<T, TId, TResult> fluentFind) where T : class where TResult : class
    {
        var cursor = await fluentFind.ToCursorAsync().ConfigureAwait(false);
        return cursor.ToList();
    }

    public static List<TResult> ToList<T, TId, TResult>(this FluentFind<T, TId, TResult> fluentFind) where T : class where TResult : class
    {
        var initialResults = fluentFind.RunAsync(null, true).ResultSync();
        var cursor = new Cursor<TResult>(initialResults, (string pageState, bool runSynchronously) => fluentFind.RunAsync(pageState, runSynchronously));
        return cursor.ToList();
    }
}