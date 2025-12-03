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
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A sort builder specifically for document operations.
/// </summary>
/// <typeparam name="T">The type of the document</typeparam>
public class DocumentSortBuilder<T> : SortBuilder<T>
{
    /// <summary>
    /// Adds a vector sort.
    /// </summary>
    /// <param name="vector">The vector to sort by.</param>
    /// <returns>The document sort builder.</returns>
    public DocumentSortBuilder<T> Vector(float[] vector)
    {
        Sorts.Add(Sort.Vector(vector));
        return this;
    }

    /// <summary>
    /// Adds a vector sort by specifying a string value to be vectorized using the collection's vectorizer.
    /// </summary>
    /// <param name="valueToVectorize">The string value to be vectorized.</param>
    /// <returns>The document sort builder.</returns>
    public DocumentSortBuilder<T> Vectorize(string valueToVectorize)
    {
        Sorts.Add(Sort.Vectorize(valueToVectorize));
        return this;
    }

    /// <inheritdoc />
    public new DocumentSortBuilder<T> Ascending(string fieldName)
    {
        base.Ascending(fieldName);
        return this;
    }

    /// <inheritdoc />
    public new DocumentSortBuilder<T> Ascending<TField>(Expression<Func<T, TField>> expression)
    {
        base.Ascending(expression);
        return this;
    }

    /// <inheritdoc />
    public new DocumentSortBuilder<T> Descending(string fieldName)
    {
        base.Descending(fieldName);
        return this;
    }

    /// <inheritdoc />
    public new DocumentSortBuilder<T> Descending<TField>(Expression<Func<T, TField>> expression)
    {
        base.Descending(expression);
        return this;
    }


    public DocumentSortBuilder<T> Lexical(string value)
    {
        Sorts.Add(Sort.Lexical(value));
        return this;
    }

    internal new DocumentSortBuilder<T> Clone()
    {
        var clone = new DocumentSortBuilder<T>();
        foreach (var sort in this.Sorts)
        {
            clone.Sorts.Add(sort.Clone());
        }
        return clone;
    }
}
