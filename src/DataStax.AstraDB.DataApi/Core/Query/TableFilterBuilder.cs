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
public class TableFilterBuilder<T> : FilterBuilder<T>
{
    // ── Logical ──────────────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.And"/>
    public TableFilter<T> And(params TableFilter<T>[] filters)
        => new LogicalTableFilter<T>(LogicalOperator.And, filters);

    /// <inheritdoc cref="FilterBuilder{T}.Or"/>
    public TableFilter<T> Or(params TableFilter<T>[] filters)
        => new LogicalTableFilter<T>(LogicalOperator.Or, filters);

    /// <inheritdoc cref="FilterBuilder{T}.Not"/>
    public TableFilter<T> Not(TableFilter<T> filter)
        => new LogicalTableFilter<T>(LogicalOperator.Not, filter);

    // ── Comparison ───────────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.Gt(string, object)"/>
    public new TableFilter<T> Gt(string fieldName, object value)
        => new TableFilter<T>(fieldName, FilterOperator.GreaterThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Gt{TField}"/>
    public new TableFilter<T> Gt<TField>(Expression<Func<T, TField>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Gte(string, object)"/>
    public new TableFilter<T> Gte(string fieldName, object value)
        => new TableFilter<T>(fieldName, FilterOperator.GreaterThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Gte{TField}"/>
    public new TableFilter<T> Gte<TField>(Expression<Func<T, TField>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lt(string, object)"/>
    public new TableFilter<T> Lt(string fieldName, object value)
        => new TableFilter<T>(fieldName, FilterOperator.LessThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lt{TField}"/>
    public new TableFilter<T> Lt<TField>(Expression<Func<T, TField>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.LessThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lte(string, object)"/>
    public new TableFilter<T> Lte(string fieldName, object value)
        => new TableFilter<T>(fieldName, FilterOperator.LessThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lte{TField}"/>
    public new TableFilter<T> Lte<TField>(Expression<Func<T, TField>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.LessThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Eq(string, object)"/>
    public new TableFilter<T> Eq(string fieldName, object value)
        => new TableFilter<T>(fieldName, FilterOperator.EqualsTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Eq{TField}"/>
    public new TableFilter<T> Eq<TField>(Expression<Func<T, TField>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.EqualsTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Ne(string, object)"/>
    public new TableFilter<T> Ne(string fieldName, object value)
        => new TableFilter<T>(fieldName, FilterOperator.NotEqualsTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Ne{TField}"/>
    public new TableFilter<T> Ne<TField>(Expression<Func<T, TField>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotEqualsTo, value);

    // ── In / Nin ─────────────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.In{T2}(string, T2[])"/>
    public new TableFilter<T> In<T2>(string fieldName, T2[] values)
        => new TableFilter<T>(fieldName, FilterOperator.In, values);

    /// <inheritdoc cref="FilterBuilder{T}.In{TField}(Expression{Func{T, TField}}, TField[])"/>
    public new TableFilter<T> In<TField>(Expression<Func<T, TField>> expression, TField[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);

    /// <inheritdoc cref="FilterBuilder{T}.In{TField}(Expression{Func{T, TField[]}}, TField[])"/>
    public new TableFilter<T> In<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);

    /// <inheritdoc cref="FilterBuilder{T}.In(string, object)"/>
    public new TableFilter<T> In(string fieldName, object value)
        => new TableFilter<T>(fieldName, FilterOperator.In, new object[] { value });

    /// <inheritdoc cref="FilterBuilder{T}.In{TField}(Expression{Func{T, TField[]}}, TField)"/>
    public new TableFilter<T> In<TField>(Expression<Func<T, TField[]>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, new object[] { value });

    /// <inheritdoc cref="FilterBuilder{T}.In{TKey, TValue}(Expression{Func{T, IDictionary{TKey, TValue}}}, ValueTuple{TKey, TValue}[])"/>
    public new TableFilter<T> In<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, pairArrays);
    }

    /// <inheritdoc cref="FilterBuilder{T}.In{TKey, TValue}(string, ValueTuple{TKey, TValue}[])"/>
    public new TableFilter<T> In<TKey, TValue>(string fieldName, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return new TableFilter<T>(fieldName, FilterOperator.In, pairArrays);
    }

    /// <inheritdoc cref="FilterBuilder{T}.Nin{T2}(string, T2[])"/>
    public new TableFilter<T> Nin<T2>(string fieldName, T2[] values)
        => new TableFilter<T>(fieldName, FilterOperator.NotIn, values);

    /// <inheritdoc cref="FilterBuilder{T}.Nin{TField}(Expression{Func{T, TField}}, TField[])"/>
    public new TableFilter<T> Nin<TField>(Expression<Func<T, TField>> expression, TField[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);

    /// <inheritdoc cref="FilterBuilder{T}.Nin{TField}(Expression{Func{T, TField[]}}, TField[])"/>
    public new TableFilter<T> Nin<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);

    /// <inheritdoc cref="FilterBuilder{T}.Nin{TField}(Expression{Func{T, TField[]}}, TField)"/>
    public new TableFilter<T> Nin<TField>(Expression<Func<T, TField[]>> expression, TField value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, new object[] { value });

    /// <inheritdoc cref="FilterBuilder{T}.Nin(string, object)"/>
    public new TableFilter<T> Nin(string field, object value)
        => new TableFilter<T>(field, FilterOperator.NotIn, new object[] { value });

    // ── Existence / Array ────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.Exists(string)"/>
    public new TableFilter<T> Exists(string fieldName)
        => new TableFilter<T>(fieldName, FilterOperator.Exists, true);

    /// <inheritdoc cref="FilterBuilder{T}.Exists{TField}"/>
    public new TableFilter<T> Exists<TField>(Expression<Func<T, IEnumerable<TField>>> expression)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Exists, true);

    /// <inheritdoc cref="FilterBuilder{T}.All{TField}(string, TField[])"/>
    public new TableFilter<T> All<TField>(string fieldName, TField[] array)
        => new TableFilter<T>(fieldName, FilterOperator.All, array);

    /// <inheritdoc cref="FilterBuilder{T}.All{TField}(Expression{Func{T, TField[]}}, TField[])"/>
    public new TableFilter<T> All<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.All, array);

    /// <inheritdoc cref="FilterBuilder{T}.AllPairs{TField}(string, TField[])"/>
    public new TableFilter<T> AllPairs<TField>(string fieldName, TField[] array)
        => new TableFilter<T>(fieldName, FilterOperator.All, array);

    /// <inheritdoc cref="FilterBuilder{T}.AllPairs{TKey, TValue}"/>
    public new TableFilter<T> AllPairs<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, IEnumerable<KeyValuePair<TKey, TValue>> pairs)
    {
        var array = pairs.Select(kv => new object[] { kv.Key, kv.Value }).ToArray();
        return new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.All, array);
    }

    /// <inheritdoc cref="FilterBuilder{T}.Size(string, int)"/>
    public new TableFilter<T> Size(string fieldName, int size)
        => new TableFilter<T>(fieldName, FilterOperator.Size, size);

    /// <inheritdoc cref="FilterBuilder{T}.Size{TField}"/>
    public new TableFilter<T> Size<TField>(Expression<Func<T, TField[]>> expression, int size)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Size, size);

    // ── Key / Compound ───────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.CompositeKey"/>
    public new TableFilter<T> CompositeKey(params PrimaryKeyFilter[] values)
    {
        var dictionary = values.ToDictionary(x => x.ColumnName, x => x.Value);
        return new TableFilter<T>(null, dictionary);
    }

    /// <inheritdoc cref="FilterBuilder{T}.CompoundKey"/>
    public new TableFilter<T> CompoundKey(PrimaryKeyFilter[] partitionColumns, Filter<T>[] clusteringColumns)
    {
        var dictionary = partitionColumns.ToDictionary(x => x.ColumnName, x => x.Value);
        foreach (var clusteringColumn in clusteringColumns)
        {
            if (clusteringColumn.Value is Filter<T> filter)
                dictionary.Add(clusteringColumn.Name, new Filter<T>(filter.Name, filter.Value));
            else
                throw new ArgumentException("Only the following filters are allowed for clustering column filters: Gt, Gte, Lt, Lte, Eq");
        }
        return new TableFilter<T>(null, dictionary);
    }

    // ── Table-specific ───────────────────────────────────────────────────────

    /// <summary>Creates a filter that matches rows where the keys of a map column contain any of the specified keys.</summary>
    public TableFilter<T> KeysIn<T2>(string fieldName, T2[] array)
        => new TableFilter<T>(fieldName, FilterOperator.Keys, new TableFilter<T>(FilterOperator.In, array));

    /// <summary>Creates a filter that matches rows where the keys of a map column contain any of the specified keys.</summary>
    public TableFilter<T> KeysIn<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TKey[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Keys, new TableFilter<T>(FilterOperator.In, array));

    /// <summary>Creates a filter that matches rows where the keys of a map column contain all of the specified keys.</summary>
    public TableFilter<T> KeysAll<T2>(string fieldName, T2[] array)
        => new TableFilter<T>(fieldName, FilterOperator.Keys, new TableFilter<T>(FilterOperator.All, array));

    /// <summary>Creates a filter that matches rows where the keys of a map column contain all of the specified keys.</summary>
    public TableFilter<T> KeysAll<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TKey[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Keys, new TableFilter<T>(FilterOperator.All, array));

    /// <summary>Creates a filter that matches rows where the keys of a map column do not contain any of the specified keys.</summary>
    public TableFilter<T> KeysNin<T2>(string fieldName, T2[] array)
        => new TableFilter<T>(fieldName, FilterOperator.Keys, new TableFilter<T>(FilterOperator.NotIn, array));

    /// <summary>Creates a filter that matches rows where the keys of a map column do not contain any of the specified keys.</summary>
    public TableFilter<T> KeysNin<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TKey[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Keys, new TableFilter<T>(FilterOperator.NotIn, array));

    /// <summary>Creates a filter that matches rows where the values of a map column contain any of the specified values.</summary>
    public TableFilter<T> ValuesIn<TValue>(string fieldName, TValue[] array)
        => new TableFilter<T>(fieldName, FilterOperator.Values, new TableFilter<T>(FilterOperator.In, array));

    /// <summary>Creates a filter that matches rows where the values of a map column contain any of the specified values.</summary>
    public TableFilter<T> ValuesIn<TKey, TVal>(Expression<Func<T, Dictionary<TKey, TVal>>> expression, TVal[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Values, new TableFilter<T>(FilterOperator.In, array));

    /// <summary>Creates a filter that matches rows where the values of a map column contain all of the specified values.</summary>
    public TableFilter<T> ValuesAll<TValue>(string fieldName, TValue[] array)
        => new TableFilter<T>(fieldName, FilterOperator.Values, new TableFilter<T>(FilterOperator.All, array));

    /// <summary>Creates a filter that matches rows where the values of a map column contain all of the specified values.</summary>
    public TableFilter<T> ValuesAll<TKey, TValue>(Expression<Func<T, Dictionary<TKey, TValue>>> expression, TValue[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Values, new TableFilter<T>(FilterOperator.All, array));

    /// <summary>Creates a filter that matches rows where the values of a map column do not contain any of the specified values.</summary>
    public TableFilter<T> ValuesNin<T2>(string fieldName, T2[] array)
        => new TableFilter<T>(fieldName, FilterOperator.Values, new TableFilter<T>(FilterOperator.NotIn, array));

    /// <summary>Creates a filter that matches rows where the values of a map column do not contain any of the specified values.</summary>
    public TableFilter<T> ValuesNin<TKey, TValue>(Expression<Func<T, Dictionary<TKey, TValue>>> expression, TValue[] array)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Values, new TableFilter<T>(FilterOperator.NotIn, array));

    /// <summary>
    /// Lexical match operator -- Matches rows where the row's lexical field value is a
    /// lexicographical match to the specified string of space-separated keywords or terms.
    /// </summary>
    public TableFilter<T> LexicalMatch(string fieldName, string value)
        => new TableFilter<T>(fieldName, FilterOperator.Match, value);

    /// <summary>
    /// Lexical match operator -- Matches rows where the row's lexical field value is a
    /// lexicographical match to the specified string of space-separated keywords or terms.
    /// </summary>
    public TableFilter<T> LexicalMatch<TKey, TValue>(Expression<Func<T, TValue>> expression, string value)
        => new TableFilter<T>(expression.GetMemberNameTree(), FilterOperator.Match, value);
}
