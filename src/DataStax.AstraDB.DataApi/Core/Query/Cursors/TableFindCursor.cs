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

using DataStax.AstraDB.DataApi.Tables;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Query.Cursors;

public class TableFindCursor<T> : FindCursor<T> where T : class
{
    private readonly Table<T> _table;

    internal TableFindCursor(
        Table<T> table,
        IFindManyOptions<T, SortBuilder<T>> options,
        CommandOptions commandOptions
    ) : base(options, commandOptions)
    {
        _table = table;
    }

    public override AbstractCursor<T> Clone()
    {
        return new TableFindCursor<T>(_table, FindOptions.Clone(), CommandOptions);
    }

    public new TableFindCursor<T> Filter(Filter<T> filter)
    {
        return (TableFindCursor<T>)base.Filter(filter);
    }

    public new TableFindCursor<T> Sort(SortBuilder<T> sort)
    {
        return (TableFindCursor<T>)base.Sort(sort);
    }

    public new TableFindCursor<T> Limit(int limit)
    {
        return (TableFindCursor<T>)base.Limit(limit);
    }

    public new TableFindCursor<T> Skip(int skip)
    {
        return (TableFindCursor<T>)base.Skip(skip);
    }

    public new TableFindCursor<T> Project(IProjectionBuilder projection)
    {
        return (TableFindCursor<T>)base.Project(projection);
    }

    public new TableFindCursor<T> IncludeSimilarity(bool include = true)
    {
        return (TableFindCursor<T>)base.IncludeSimilarity(include);
    }

    public new TableFindCursor<T> IncludeSortVector(bool include = true)
    {
        return (TableFindCursor<T>)base.IncludeSortVector(include);
    }

    internal override FindCursor<T> CloneWithOptions(IFindManyOptions<T, SortBuilder<T>> options)
    {
        return new TableFindCursor<T>(_table, options, CommandOptions);
    }

    protected override async Task<FindPage<T>> FetchPageInternalAsync(bool runSynchronously)
    {
        var response = await _table.RunFindManyAsync<T>(
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
