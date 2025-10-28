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
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A sort builder specifically for table operations.
/// </summary>
/// <typeparam name="T">The type of the document</typeparam>
public class TableSortBuilder<T> : SortBuilder<T>
{
    /// <summary>
    /// Adds a vector sort.
    /// </summary>
    /// <param name="fieldName">The name of the field to sort by.</param>
    /// <param name="vector">The vector to sort by.</param>
    /// <returns>The table sort builder.</returns>
    public TableSortBuilder<T> Vector(string fieldName, float[] vector)
    {
        Sorts.Add(new Sort(fieldName, vector));
        return this;
    }

    /// <summary>
    /// Adds a vector sort.
    /// </summary>
    /// <param name="expression">The expression representing the sort field.</param>
    /// <param name="vector">The vector to sort by.</param>
    /// <returns>The table sort builder.</returns>
    public TableSortBuilder<T> Vector<TField>(Expression<Func<T, TField>> expression, float[] vector)
    {
        Sorts.Add(new Sort(expression.GetMemberNameTree(), vector));
        return this;
    }

    /// <summary>
    /// Adds a vector sort by specifying a string value to be vectorized using the collection's vectorizer.
    /// </summary>
    /// <param name="fieldName">The name of the field to sort by.</param>
    /// <param name="valueToVectorize">The string value to be vectorized.</param>
    /// <returns>The table sort builder.</returns>
    public TableSortBuilder<T> Vectorize(string fieldName, string valueToVectorize)
    {
        Sorts.Add(new Sort(fieldName, valueToVectorize));
        return this;
    }

    /// <summary>
    /// Adds a vector sort by specifying a string value to be vectorized using the collection's vectorizer.
    /// </summary>
    /// <typeparam name="TField">The type of the field to sort by.</typeparam>
    /// <param name="expression">The expression representing the sort field.</param>
    /// <param name="valueToVectorize">The string value to be vectorized.</param>
    /// <returns>The table sort builder.</returns>
    public TableSortBuilder<T> Vectorize<TField>(Expression<Func<T, TField>> expression, string valueToVectorize)
    {
        Sorts.Add(new Sort(expression.GetMemberNameTree(), valueToVectorize));
        return this;
    }

    /// <inheritdoc />
    public new TableSortBuilder<T> Ascending(string fieldName)
    {
        base.Ascending(fieldName);
        return this;
    }

    /// <inheritdoc />
    public new TableSortBuilder<T> Ascending<TField>(Expression<Func<T, TField>> expression)
    {
        base.Ascending(expression);
        return this;
    }

    /// <inheritdoc />
    public new TableSortBuilder<T> Descending(string fieldName)
    {
        base.Descending(fieldName);
        return this;
    }

    /// <inheritdoc />
    public new TableSortBuilder<T> Descending<TField>(Expression<Func<T, TField>> expression)
    {
        base.Descending(expression);
        return this;
    }

    /// <summary>
    /// Adds a lexical sort.
    /// </summary>
    /// <param name="column"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public TableSortBuilder<T> Lexical<TField>(Expression<Func<T, TField>> column, string value)
    {
        Sorts.Add(Sort<T>.TableLexical(column, value));
        return this;
    }

    internal new TableSortBuilder<T> Clone()
    {
        var clone = new TableSortBuilder<T>();
        foreach (var sort in this.Sorts)
        {
            clone.Sorts.Add(sort.Clone());
        }
        return clone;
    }
}
