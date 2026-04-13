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

public class CollectionFindCursor<T> : CollectionFindCursor<T, T> where T : class
{
    internal CollectionFindCursor(IFindManyOptions<T, DocumentSortBuilder<T>> options, CommandOptions commandOptions, FetchPageFunc<T, CollectionFindCursor<T, T>> fetchPage) 
        : base(options, commandOptions, fetchPage) { }
}

public class CollectionFindCursor<T, TResult> : FindCursor<T, TResult, DocumentSortBuilder<T>, CollectionFindCursor<T, TResult>>
    where T : class
    where TResult : class
{
    internal CollectionFindCursor(
        IFindManyOptions<T, DocumentSortBuilder<T>> options,
        CommandOptions commandOptions,
        FetchPageFunc<TResult, CollectionFindCursor<T, TResult>> fetchPage
    ) : base(options, commandOptions, fetchPage) { }

    public override AbstractCursor<TResult> Clone()
    {
        return new CollectionFindCursor<T, TResult>(FindOptions.Clone(), CommandOptions, FetchPageFunc);
    }

    internal override CollectionFindCursor<T, TResult> CloneWithOptions(IFindManyOptions<T, DocumentSortBuilder<T>> options)
    {
        return new CollectionFindCursor<T, TResult>(options, CommandOptions, FetchPageFunc);
    }
}
