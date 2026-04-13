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

namespace DataStax.AstraDB.DataApi.Core.Enumeration;

public class TableFindCursor<T> : TableFindCursor<T, T> where T : class
{
    internal TableFindCursor(IFindManyOptions<T, TableSortBuilder<T>> options, CommandOptions commandOptions, FetchPageFunc<T, TableFindCursor<T, T>> fetchPage) 
        : base(options, commandOptions, fetchPage) { }
}

public class TableFindCursor<T, TResult> : FindCursor<T, TResult, TableSortBuilder<T>, TableFindCursor<T, TResult>>
    where T : class
    where TResult : class
{
    internal TableFindCursor(
        IFindManyOptions<T, TableSortBuilder<T>> options,
        CommandOptions commandOptions,
        FetchPageFunc<TResult, TableFindCursor<T, TResult>> fetchPage
    ) : base(options, commandOptions, fetchPage)
    {
    }

    public override AbstractCursor<TResult> Clone()
    {
        return new TableFindCursor<T, TResult>(FindOptions.Clone(), CommandOptions, FetchPageFunc);
    }

    internal override TableFindCursor<T, TResult> CloneWithOptions(IFindManyOptions<T, TableSortBuilder<T>> options)
    {
        return new TableFindCursor<T, TResult>(options, CommandOptions, FetchPageFunc);
    }
}
