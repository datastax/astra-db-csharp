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

public class Sort
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

    public static Sort Ascending(string field) => new(field, SortAscending);

    public static Sort Descending(string field) => new(field, SortDescending);

    public static Sort Vector(float[] vector) => new(DataApiKeywords.Vector, vector);

    public static Sort Vectorize(string valueToVectorize) => new(DataApiKeywords.Vectorize, valueToVectorize);
}

public class Sort<T> : Sort
{
    internal Sort(string sortKey, object value) : base(sortKey, value) { }

    public static Sort Ascending<TField>(Expression<Func<T, TField>> expression)
    {
        return new Sort<T>(expression.GetMemberNameTree(), SortAscending);
    }

    public static Sort Descending<TField>(Expression<Func<T, TField>> expression)
    {
        return new Sort<T>(expression.GetMemberNameTree(), SortDescending);
    }
}
