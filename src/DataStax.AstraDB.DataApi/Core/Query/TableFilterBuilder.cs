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
using System.Linq;
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

    /// <summary>
    /// Build a composite key filter using a dictionary of primary key names and the values to match.
    /// </summary>
    /// <param name="values">The primary key column name/value pairs.</param>
    /// <returns>The filter</returns>
    public TableFilter<T> CompositeKey(params PrimaryKeyFilter[] values)
    {
        var dictionary = values.ToDictionary(x => x.ColumnName, x => x.Value);
        return Make(null, dictionary);
    }

    /// <summary>
    /// Build a compound key filter using partition key columns and range/equality filters on clustering columns.
    /// </summary>
    /// <param name="partitionColumns">Exact partition key values.</param>
    /// <param name="clusteringColumns">
    /// Range or equality filters on clustering columns. Only Gt, Gte, Lt, Lte, and Eq are permitted;
    /// any other operator throws <see cref="ArgumentException"/>.
    /// </param>
    /// <returns>The filter</returns>
    public TableFilter<T> CompoundKey(PrimaryKeyFilter[] partitionColumns, Filter<T>[] clusteringColumns)
    {
        var dictionary = partitionColumns.ToDictionary(x => x.ColumnName, x => x.Value);
        foreach (var clusteringColumn in clusteringColumns)
        {
            if (clusteringColumn.Value is Filter<T> filter && _allowedClusteringOps.Contains(filter.Name))
                dictionary.Add(clusteringColumn.Name, new Filter<T>(filter.Name, filter.Value));
            else
                throw new ArgumentException("Only the following filters are allowed for clustering column filters: Gt, Gte, Lt, Lte, Eq");
        }
        return Make(null, dictionary);
    }

    /// <inheritdoc cref="CompoundKey(PrimaryKeyFilter[], Filter{T}[])"/>
    public TableFilter<T> CompoundKey(PrimaryKeyFilterBuilder<T> partitionColumns, Filter<T>[] clusteringColumns)
        => CompoundKey(partitionColumns.Build(), clusteringColumns);

    private static readonly HashSet<string> _allowedClusteringOps = new()
    {
        FilterOperator.GreaterThan,
        FilterOperator.GreaterThanOrEqualTo,
        FilterOperator.LessThan,
        FilterOperator.LessThanOrEqualTo,
        FilterOperator.EqualsTo,
    };
}
