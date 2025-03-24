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

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class SortBuilder<T>
{
    private List<Sort> _sorts = new List<Sort>();

    internal List<Sort> Sorts => _sorts;

    public SortBuilder<T> Ascending(string fieldName)
    {
        _sorts.Add(Sort.Ascending(fieldName));
        return this;
    }

    public SortBuilder<T> Ascending<TField>(Expression<Func<T, TField>> expression)
    {
        _sorts.Add(Sort<T>.Ascending(expression));
        return this;
    }

    public SortBuilder<T> Descending(string fieldName)
    {
        _sorts.Add(Sort.Descending(fieldName));
        return this;
    }

    public SortBuilder<T> Descending<TField>(Expression<Func<T, TField>> expression)
    {
        _sorts.Add(Sort<T>.Descending(expression));
        return this;
    }

    public SortBuilder<T> Vector(float[] vector)
    {
        _sorts.Add(Sort.Vector(vector));
        return this;
    }

    public SortBuilder<T> Vectorize(string valueToVectorize)
    {
        _sorts.Add(Sort.Vectorize(valueToVectorize));
        return this;
    }

}