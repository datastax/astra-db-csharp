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
/// A fluent builder for constructing a set of primary key column filters.
/// Obtain an instance via <c>Builders&lt;T&gt;.PrimaryKey</c>.
/// </summary>
/// <typeparam name="T">The type of the row or document.</typeparam>
public class PrimaryKeyFilterBuilder<T>
{
    private readonly List<PrimaryKeyFilter> _filters = new();

    /// <summary>
    /// Adds a primary key column filter using a string column name.
    /// </summary>
    /// <param name="columnName">The name of the primary key column.</param>
    /// <param name="value">The value to match.</param>
    public PrimaryKeyFilterBuilder<T> Add(string columnName, object value)
    {
        _filters.Add(new PrimaryKeyFilter(columnName, value));
        return this;
    }

    /// <summary>
    /// Adds a primary key column filter using a strongly-typed member expression.
    /// </summary>
    /// <typeparam name="TValue">The type of the column value.</typeparam>
    /// <param name="expression">An expression identifying the primary key column.</param>
    /// <param name="value">The value to match.</param>
    public PrimaryKeyFilterBuilder<T> Add<TValue>(Expression<Func<T, TValue>> expression, TValue value)
    {
        _filters.Add(new PrimaryKeyFilter(expression.GetMemberNameTree(), value));
        return this;
    }

    /// <summary>
    /// Returns the accumulated primary key filters as an array.
    /// </summary>
    public PrimaryKeyFilter[] Build() => _filters.ToArray();

    /// <summary>
    /// Implicitly converts the builder to a <see cref="PrimaryKeyFilter"/> array,
    /// allowing it to be passed directly to <c>CompositeKey</c> and <c>CompoundKey</c>.
    /// </summary>
    public static implicit operator PrimaryKeyFilter[](PrimaryKeyFilterBuilder<T> builder)
        => builder.Build();
}
