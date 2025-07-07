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

using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A utility for building sorting specifications for an operation.
/// </summary>
/// <typeparam name="T">The type of the document</typeparam>
public class SortBuilder<T>
{
    internal List<Sort> Sorts { get; set; } = new List<Sort>();

    /// <summary>
    /// Adds an ascending sort.
    /// </summary>
    /// <param name="fieldName">The name of the field to sort by.</param>
    /// <returns>The sort builder.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Ascending{TField}"/> instead.
    /// </remarks>
    public SortBuilder<T> Ascending(string fieldName)
    {
        Sorts.Add(Sort.Ascending(fieldName));
        return this;
    }

    /// <summary>
    /// Adds an ascending sort.
    /// </summary>
    /// <typeparam name="TField">The type of the field to sort by.</typeparam>
    /// <param name="expression">The expression representing the sort field.</param>
    /// <returns>The sort builder.</returns>
    public SortBuilder<T> Ascending<TField>(Expression<Func<T, TField>> expression)
    {
        Sorts.Add(Sort<T>.Ascending(expression));
        return this;
    }

    /// <summary>
    /// Adds a descending sort.
    /// </summary>
    /// <param name="fieldName">The name of the field to sort by.</param>
    /// <returns>The sort builder.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Descending{TField}"/> instead.
    /// </remarks>
    public SortBuilder<T> Descending(string fieldName)
    {
        Sorts.Add(Sort.Descending(fieldName));
        return this;
    }

    /// <summary>
    /// Adds a descending sort.
    /// </summary>
    /// <typeparam name="TField">The type of the field to sort by.</typeparam>
    /// <param name="expression">The expression representing the sort field.</param>
    /// <returns>The sort builder.</returns>
    public SortBuilder<T> Descending<TField>(Expression<Func<T, TField>> expression)
    {
        Sorts.Add(Sort<T>.Descending(expression));
        return this;
    }

    internal SortBuilder<T> Clone()
    {
        var clone = new SortBuilder<T>();
        foreach (var sort in this.Sorts)
        {
            clone.Sorts.Add(sort.Clone());
        }
        return clone;
    }
}