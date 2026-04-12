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

using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Cursors;

public class FindPage<T>
{
    public FindPage(string nextPageState, IReadOnlyList<T> result, float[] sortVector)
    {
        NextPageState = nextPageState;
        Result = new List<T>(result);
        SortVector = sortVector;
    }
    
    public string NextPageState { get; }
    public List<T> Result { get; }
    public float[] SortVector { get; }
}

public abstract class FindCursor<T, TCursor> : AbstractCursor<T> where TCursor : FindCursor<T, TCursor>
{
    internal IFindManyOptions<T, SortBuilder<T>> FindOptions { get; }
    internal CommandOptions CommandOptions { get; }
    
    private FindPage<T> _currentPage;
    protected override List<T> _buffer => _currentPage?.Result;
    
    internal FindCursor(IFindManyOptions<T, SortBuilder<T>> options, CommandOptions commandOptions)
    {
        FindOptions = options.Clone();
        CommandOptions = commandOptions;
    }
    
    public TCursor Filter(Filter<T> filter)
    {
        return (TCursor)UpdateOptions(options => options.Filter = filter);
    }
    
    public TCursor Sort(SortBuilder<T> sort)
    {
        return (TCursor)UpdateOptions(options => options.Sort = sort);
    }
    
    public TCursor Limit(int limit)
    {
        return (TCursor)UpdateOptions(options => options.Limit = limit);
    }
    
    public TCursor Skip(int skip)
    {
        return (TCursor)UpdateOptions(options => options.Skip = skip);
    }
    
    public TCursor Project(IProjectionBuilder projection) // TODO add a version with a NewT generic
    {
        return (TCursor)UpdateOptions(options => options.Projection = projection);
    }
    
    public TCursor IncludeSimilarity(bool include = true)
    {
        return (TCursor)UpdateOptions(options => options.IncludeSimilarity = include);
    }
    
    public TCursor IncludeSortVector(bool include = true)
    {
        return (TCursor)UpdateOptions(options => options.IncludeSortVector = include);
    }
    
    public float[] GetSortVector()
    {
        return GetSortVectorAsync(CancellationToken.None, true).ResultSync();
    }
    
    public async Task<float[]> GetSortVectorAsync(CancellationToken cancellationToken = default)
    {
        return await GetSortVectorAsync(cancellationToken, false);
    }
    
    private async Task<float[]> GetSortVectorAsync(CancellationToken cancellationToken, bool runSynchronously)
    {
        if (_currentPage == null && FindOptions.IncludeSortVector == true)
        {
            await MoveNextAsync(cancellationToken, peek: true, runSynchronously).ConfigureAwait(false);
        }
        return _currentPage?.SortVector;
    }
    
    public override void Rewind()
    {
        base.Rewind();
        _currentPage = null;
    }
    
    public override void Dispose()
    {
        base.Dispose();
        _currentPage = null;
    }
    
    protected override async Task<bool> FetchNextPageAsync(CancellationToken cancellationToken, bool runSynchronously)
    {
        if (cancellationToken != CancellationToken.None)
        {
            CommandOptions.BulkOperationCancellationToken = cancellationToken;
        }

        var page = await FetchPageInternalAsync(runSynchronously).ConfigureAwait(false);
        FindOptions.PageState = page.NextPageState;
        _currentPage = page;
        return page.NextPageState != null;
    }

    private FindCursor<T, TCursor> UpdateOptions(Action<IFindManyOptions<T, SortBuilder<T>>> optionsUpdater)
    {
        var newOptions = FindOptions.Clone();
        optionsUpdater(newOptions);
        return CloneWithOptions(newOptions);
    }
    
    internal abstract FindCursor<T, TCursor> CloneWithOptions(IFindManyOptions<T, SortBuilder<T>> options);
    
    protected abstract Task<FindPage<T>> FetchPageInternalAsync(bool runSynchronously);
}
