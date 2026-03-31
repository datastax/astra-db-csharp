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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A builder for creating filter definitions for collection queries.
/// Obtain an instance via <c>Builders&lt;T&gt;.CollectionFilter</c>.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class CollectionFilterBuilder<T> : FilterBuilder<T>
{
    // ── Logical ──────────────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.And"/>
    public CollectionFilter<T> And(params CollectionFilter<T>[] filters)
        => new LogicalCollectionFilter<T>(LogicalOperator.And, filters);

    /// <inheritdoc cref="FilterBuilder{T}.Or"/>
    public CollectionFilter<T> Or(params CollectionFilter<T>[] filters)
        => new LogicalCollectionFilter<T>(LogicalOperator.Or, filters);

    /// <inheritdoc cref="FilterBuilder{T}.Not"/>
    public CollectionFilter<T> Not(CollectionFilter<T> filter)
        => new LogicalCollectionFilter<T>(LogicalOperator.Not, filter);

    // ── Comparison ───────────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.Gt(string, object)"/>
    public new CollectionFilter<T> Gt(string fieldName, object value)
        => new CollectionFilter<T>(fieldName, FilterOperator.GreaterThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Gt{TField}"/>
    public new CollectionFilter<T> Gt<TField>(Expression<Func<T, TField>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Gte(string, object)"/>
    public new CollectionFilter<T> Gte(string fieldName, object value)
        => new CollectionFilter<T>(fieldName, FilterOperator.GreaterThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Gte{TField}"/>
    public new CollectionFilter<T> Gte<TField>(Expression<Func<T, TField>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lt(string, object)"/>
    public new CollectionFilter<T> Lt(string fieldName, object value)
        => new CollectionFilter<T>(fieldName, FilterOperator.LessThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lt{TField}"/>
    public new CollectionFilter<T> Lt<TField>(Expression<Func<T, TField>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.LessThan, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lte(string, object)"/>
    public new CollectionFilter<T> Lte(string fieldName, object value)
        => new CollectionFilter<T>(fieldName, FilterOperator.LessThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Lte{TField}"/>
    public new CollectionFilter<T> Lte<TField>(Expression<Func<T, TField>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.LessThanOrEqualTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Eq(string, object)"/>
    public new CollectionFilter<T> Eq(string fieldName, object value)
        => new CollectionFilter<T>(fieldName, FilterOperator.EqualsTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Eq{TField}"/>
    public new CollectionFilter<T> Eq<TField>(Expression<Func<T, TField>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.EqualsTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Ne(string, object)"/>
    public new CollectionFilter<T> Ne(string fieldName, object value)
        => new CollectionFilter<T>(fieldName, FilterOperator.NotEqualsTo, value);

    /// <inheritdoc cref="FilterBuilder{T}.Ne{TField}"/>
    public new CollectionFilter<T> Ne<TField>(Expression<Func<T, TField>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotEqualsTo, value);

    // ── In / Nin ─────────────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.In{T2}(string, T2[])"/>
    public new CollectionFilter<T> In<T2>(string fieldName, T2[] values)
        => new CollectionFilter<T>(fieldName, FilterOperator.In, values);

    /// <inheritdoc cref="FilterBuilder{T}.In{TField}(Expression{Func{T, TField}}, TField[])"/>
    public new CollectionFilter<T> In<TField>(Expression<Func<T, TField>> expression, TField[] array)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);

    /// <inheritdoc cref="FilterBuilder{T}.In{TField}(Expression{Func{T, TField[]}}, TField[])"/>
    public new CollectionFilter<T> In<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);

    /// <inheritdoc cref="FilterBuilder{T}.In(string, object)"/>
    public new CollectionFilter<T> In(string fieldName, object value)
        => new CollectionFilter<T>(fieldName, FilterOperator.In, new object[] { value });

    /// <inheritdoc cref="FilterBuilder{T}.In{TField}(Expression{Func{T, TField[]}}, TField)"/>
    public new CollectionFilter<T> In<TField>(Expression<Func<T, TField[]>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, new object[] { value });

    /// <inheritdoc cref="FilterBuilder{T}.In{TKey, TValue}(Expression{Func{T, IDictionary{TKey, TValue}}}, ValueTuple{TKey, TValue}[])"/>
    public new CollectionFilter<T> In<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.In, pairArrays);
    }

    /// <inheritdoc cref="FilterBuilder{T}.In{TKey, TValue}(string, ValueTuple{TKey, TValue}[])"/>
    public new CollectionFilter<T> In<TKey, TValue>(string fieldName, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return new CollectionFilter<T>(fieldName, FilterOperator.In, pairArrays);
    }

    /// <inheritdoc cref="FilterBuilder{T}.Nin{T2}(string, T2[])"/>
    public new CollectionFilter<T> Nin<T2>(string fieldName, T2[] values)
        => new CollectionFilter<T>(fieldName, FilterOperator.NotIn, values);

    /// <inheritdoc cref="FilterBuilder{T}.Nin{TField}(Expression{Func{T, TField}}, TField[])"/>
    public new CollectionFilter<T> Nin<TField>(Expression<Func<T, TField>> expression, TField[] array)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);

    /// <inheritdoc cref="FilterBuilder{T}.Nin{TField}(Expression{Func{T, TField[]}}, TField[])"/>
    public new CollectionFilter<T> Nin<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);

    /// <inheritdoc cref="FilterBuilder{T}.Nin{TField}(Expression{Func{T, TField[]}}, TField)"/>
    public new CollectionFilter<T> Nin<TField>(Expression<Func<T, TField[]>> expression, TField value)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, new object[] { value });

    /// <inheritdoc cref="FilterBuilder{T}.Nin(string, object)"/>
    public new CollectionFilter<T> Nin(string field, object value)
        => new CollectionFilter<T>(field, FilterOperator.NotIn, new object[] { value });

    // ── Existence / Array ────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.Exists(string)"/>
    public new CollectionFilter<T> Exists(string fieldName)
        => new CollectionFilter<T>(fieldName, FilterOperator.Exists, true);

    /// <inheritdoc cref="FilterBuilder{T}.Exists{TField}"/>
    public new CollectionFilter<T> Exists<TField>(Expression<Func<T, IEnumerable<TField>>> expression)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.Exists, true);

    /// <inheritdoc cref="FilterBuilder{T}.All{TField}(string, TField[])"/>
    public new CollectionFilter<T> All<TField>(string fieldName, TField[] array)
        => new CollectionFilter<T>(fieldName, FilterOperator.All, array);

    /// <inheritdoc cref="FilterBuilder{T}.All{TField}(Expression{Func{T, TField[]}}, TField[])"/>
    public new CollectionFilter<T> All<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.All, array);

    /// <inheritdoc cref="FilterBuilder{T}.AllPairs{TField}(string, TField[])"/>
    public new CollectionFilter<T> AllPairs<TField>(string fieldName, TField[] array)
        => new CollectionFilter<T>(fieldName, FilterOperator.All, array);

    /// <inheritdoc cref="FilterBuilder{T}.AllPairs{TKey, TValue}"/>
    public new CollectionFilter<T> AllPairs<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, IEnumerable<KeyValuePair<TKey, TValue>> pairs)
    {
        var array = pairs.Select(kv => new object[] { kv.Key, kv.Value }).ToArray();
        return new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.All, array);
    }

    /// <inheritdoc cref="FilterBuilder{T}.Size(string, int)"/>
    public new CollectionFilter<T> Size(string fieldName, int size)
        => new CollectionFilter<T>(fieldName, FilterOperator.Size, size);

    /// <inheritdoc cref="FilterBuilder{T}.Size{TField}"/>
    public new CollectionFilter<T> Size<TField>(Expression<Func<T, TField[]>> expression, int size)
        => new CollectionFilter<T>(expression.GetMemberNameTree(), FilterOperator.Size, size);

    // ── Key / Compound ───────────────────────────────────────────────────────

    /// <inheritdoc cref="FilterBuilder{T}.CompositeKey"/>
    public new CollectionFilter<T> CompositeKey(params PrimaryKeyFilter[] values)
    {
        var dictionary = values.ToDictionary(x => x.ColumnName, x => x.Value);
        return new CollectionFilter<T>(null, dictionary);
    }

    /// <inheritdoc cref="FilterBuilder{T}.CompoundKey"/>
    public new CollectionFilter<T> CompoundKey(PrimaryKeyFilter[] partitionColumns, Filter<T>[] clusteringColumns)
    {
        var dictionary = partitionColumns.ToDictionary(x => x.ColumnName, x => x.Value);
        foreach (var clusteringColumn in clusteringColumns)
        {
            if (clusteringColumn.Value is Filter<T> filter)
                dictionary.Add(clusteringColumn.Name, new Filter<T>(filter.Name, filter.Value));
            else
                throw new ArgumentException("Only the following filters are allowed for clustering column filters: Gt, Gte, Lt, Lte, Eq");
        }
        return new CollectionFilter<T>(null, dictionary);
    }

    // ── Collection-specific ──────────────────────────────────────────────────

    /// <summary>
    /// Lexical match operator -- Matches documents where the document's lexical field value is a
    /// lexicographical match to the specified string of space-separated keywords or terms.
    /// </summary>
    public CollectionFilter<T> LexicalMatch(string value)
        => new CollectionFilter<T>(DataApiKeywords.Lexical, FilterOperator.Match, value);
}
