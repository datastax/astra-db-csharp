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

using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

internal class Sort
{
    internal const int SortAscending = 1;
    internal const int SortDescending = -1;

    internal string Name { get; set; }
    internal object Value { get; set; }

    internal Sort(string sortKey, object value)
    {
        Name = sortKey;
        Value = value;
    }

    internal static Sort Ascending(string field) => new(field, SortAscending);

    internal static Sort Descending(string field) => new(field, SortDescending);

    internal static Sort Vector(float[] vector) => new(DataApiKeywords.Vector, vector);

    internal static Sort Vectorize(string valueToVectorize) => new(DataApiKeywords.Vectorize, valueToVectorize);
}

internal class Sort<T> : Sort
{
    internal Sort(string sortKey, object value) : base(sortKey, value) { }

    internal static Sort Ascending<TField>(Expression<Func<T, TField>> expression)
    {
        return new Sort<T>(expression.GetMemberNameTree(), SortAscending);
    }

    internal static Sort Descending<TField>(Expression<Func<T, TField>> expression)
    {
        return new Sort<T>(expression.GetMemberNameTree(), SortDescending);
    }
}
