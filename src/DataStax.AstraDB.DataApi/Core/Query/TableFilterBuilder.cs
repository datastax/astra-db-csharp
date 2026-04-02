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
/// A builder for creating filter definitions for table queries.
/// Obtain an instance via <c>Builders&lt;T&gt;.TableFilter</c>.
/// </summary>
/// <typeparam name="T">The type of the rows in the table.</typeparam>
public class TableFilterBuilder<T> : FilterBuilder<T, TableFilter<T>>
{
    /// <inheritdoc/>
    protected override TableFilter<T> Make(string name, object value) => new(name, value);

    /// <summary>Creates a filter that matches rows where the keys of a map column contain any of the specified keys.</summary>
    public TableFilter<T> KeysIn<T2>(string fieldName, T2[] array)
        => Make(fieldName, Make(FilterOperator.Keys, Make(FilterOperator.In, array)));

    /// <summary>Creates a filter that matches rows where the keys of a map column contain any of the specified keys.</summary>
    public TableFilter<T> KeysIn<TKey, TVal>(Expression<Func<T, IDictionary<TKey, TVal>>> expression, TKey[] array)
        => Make(expression.GetMemberNameTree(), Make(FilterOperator.Keys, Make(FilterOperator.In, array)));

    /// <summary>Creates a filter that matches rows where the keys of a map column contain all of the specified keys.</summary>
    public TableFilter<T> KeysAll<T2>(string fieldName, T2[] array)
        => Make(fieldName, Make(FilterOperator.Keys, Make(FilterOperator.All, array)));

    /// <summary>Creates a filter that matches rows where the keys of a map column contain all of the specified keys.</summary>
    public TableFilter<T> KeysAll<TKey, TVal>(Expression<Func<T, IDictionary<TKey, TVal>>> expression, TKey[] array)
        => Make(expression.GetMemberNameTree(), Make(FilterOperator.Keys, Make(FilterOperator.All, array)));

    /// <summary>Creates a filter that matches rows where the keys of a map column do not contain any of the specified keys.</summary>
    public TableFilter<T> KeysNin<T2>(string fieldName, T2[] array)
        => Make(fieldName, Make(FilterOperator.Keys, Make(FilterOperator.NotIn, array)));

    /// <summary>Creates a filter that matches rows where the keys of a map column do not contain any of the specified keys.</summary>
    public TableFilter<T> KeysNin<TKey, TVal>(Expression<Func<T, IDictionary<TKey, TVal>>> expression, TKey[] array)
        => Make(expression.GetMemberNameTree(), Make(FilterOperator.Keys, Make(FilterOperator.NotIn, array)));

    /// <summary>Creates a filter that matches rows where the values of a map column contain any of the specified values.</summary>
    public TableFilter<T> ValuesIn<TValue>(string fieldName, TValue[] array)
        => Make(fieldName, Make(FilterOperator.Values, Make(FilterOperator.In, array)));

    /// <summary>Creates a filter that matches rows where the values of a map column contain any of the specified values.</summary>
    public TableFilter<T> ValuesIn<TKey, TVal>(Expression<Func<T, IDictionary<TKey, TVal>>> expression, TVal[] array)
        => Make(expression.GetMemberNameTree(), Make(FilterOperator.Values, Make(FilterOperator.In, array)));

    /// <summary>Creates a filter that matches rows where the values of a map column contain all of the specified values.</summary>
    public TableFilter<T> ValuesAll<TValue>(string fieldName, TValue[] array)
        => Make(fieldName, Make(FilterOperator.Values, Make(FilterOperator.All, array)));

    /// <summary>Creates a filter that matches rows where the values of a map column contain all of the specified values.</summary>
    public TableFilter<T> ValuesAll<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, TValue[] array)
        => Make(expression.GetMemberNameTree(), Make(FilterOperator.Values, Make(FilterOperator.All, array)));

    /// <summary>Creates a filter that matches rows where the values of a map column do not contain any of the specified values.</summary>
    public TableFilter<T> ValuesNin<T2>(string fieldName, T2[] array)
        => Make(fieldName, Make(FilterOperator.Values, Make(FilterOperator.NotIn, array)));

    /// <summary>Creates a filter that matches rows where the values of a map column do not contain any of the specified values.</summary>
    public TableFilter<T> ValuesNin<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, TValue[] array)
        => Make(expression.GetMemberNameTree(), Make(FilterOperator.Values, Make(FilterOperator.NotIn, array)));

    /// <summary>
    /// Lexical match operator -- Matches rows where the row's lexical field value is a
    /// lexicographical match to the specified string of space-separated keywords or terms.
    /// </summary>
    public TableFilter<T> LexicalMatch(string fieldName, string value)
        => Make(fieldName, Make(FilterOperator.Match, value));

    /// <summary>
    /// Lexical match operator -- Matches rows where the row's lexical field value is a
    /// lexicographical match to the specified string of space-separated keywords or terms.
    /// </summary>
    public TableFilter<T> LexicalMatch<TValue>(Expression<Func<T, TValue>> expression, string value)
        => Make(expression.GetMemberNameTree(), Make(FilterOperator.Match, value));
}
