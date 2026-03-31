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
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query.Cursors;

public class CollectionFindCursor<T> : FindCursor<T> where T : class
{
    private readonly Collection<T> _collection;

    internal CollectionFindCursor(
        Collection<T> collection,
        IFindManyOptions<T, SortBuilder<T>> options,
        CommandOptions commandOptions
    ) : base(options, commandOptions)
    {
        _collection = collection;
    }

    public override AbstractCursor<T> Clone()
    {
        return new CollectionFindCursor<T>(_collection, FindOptions.Clone(), CommandOptions);
    }

    public new CollectionFindCursor<T> Filter(Filter<T> filter)
    {
        return (CollectionFindCursor<T>)base.Filter(filter);
    }

    public new CollectionFindCursor<T> Sort(SortBuilder<T> sort)
    {
        return (CollectionFindCursor<T>)base.Sort(sort);
    }

    public new CollectionFindCursor<T> Limit(int limit)
    {
        return (CollectionFindCursor<T>)base.Limit(limit);
    }

    public new CollectionFindCursor<T> Skip(int skip)
    {
        return (CollectionFindCursor<T>)base.Skip(skip);
    }

    public new CollectionFindCursor<T> Project(IProjectionBuilder projection)
    {
        return (CollectionFindCursor<T>)base.Project(projection);
    }

    public new CollectionFindCursor<T> IncludeSimilarity(bool include = true)
    {
        return (CollectionFindCursor<T>)base.IncludeSimilarity(include);
    }

    public new CollectionFindCursor<T> IncludeSortVector(bool include = true)
    {
        return (CollectionFindCursor<T>)base.IncludeSortVector(include);
    }

    internal override FindCursor<T> CloneWithOptions(IFindManyOptions<T, SortBuilder<T>> options)
    {
        return new CollectionFindCursor<T>(_collection, options, CommandOptions);
    }

    protected override async Task<FindPage<T>> FetchPageInternalAsync(bool runSynchronously)
    {
        var response = await _collection.RunFindManyAsync<T>(
            FindOptions,
            CommandOptions,
            runSynchronously
        ).ConfigureAwait(false);

        return new FindPage<T>(
            response.Data.NextPageState,
            response.Data.Items,
            response.Status.SortVector
        );
    }
}
