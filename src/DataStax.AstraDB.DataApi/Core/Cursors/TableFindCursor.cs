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
using DataStax.AstraDB.DataApi.Tables;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Cursors;

public class TableFindCursor<T> : FindCursor<T, TableFindCursor<T>> where T : class
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

    internal override FindCursor<T, TableFindCursor<T>> CloneWithOptions(IFindManyOptions<T, SortBuilder<T>> options)
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
