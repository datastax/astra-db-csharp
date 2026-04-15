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
/// Base class for all filter builders. Provides all shared filter construction methods.
/// Subclasses supply a <see cref="Make(string, object)"/> factory so every method returns the correct
/// strongly-typed filter (<typeparamref name="TFilter"/>) without method hiding.
/// </summary>
/// <typeparam name="T">The type of the document or row being filtered.</typeparam>
/// <typeparam name="TFilter">The concrete filter type produced by this builder.</typeparam>
public abstract class FilterBuilder<T, TFilter> where TFilter : Filter<T>
{
    /// <summary>
    /// Creates a filter node with the given name and value.
    /// All builder methods delegate to this factory.
    /// </summary>
    protected abstract TFilter Make(string name, object value);

    private TFilter Make(LogicalOperator op, object value)
        => Make(op.ToApiString(), value);

    /// <summary>
    /// Internal helper: wraps value in an operator node, then wraps that in a field node.
    /// </summary>
    /// <param name="fieldName"></param>
    /// <param name="op"></param>
    /// <param name="value"></param>
    /// <remarks>
    /// Produces: TFilter{ Name=fieldName, Value=TFilter{ Name=op, Value=value } }
    /// which serialises as  { "fieldName": { "op": value } }
    /// </remarks>
    /// <returns></returns>
    protected TFilter MakeOp(string fieldName, string op, object value)
        => Make(fieldName, Make(op, value));

    /// <summary>
    /// Create an empty filter with no conditions (matching everything)
    /// </summary>
    public TFilter Empty() => Make(string.Empty, new Dictionary<string, object>());

    /// <summary>
    /// Logical AND operator for combining multiple filters.
    /// </summary>
    /// <param name="filters">Array of filters to combine</param>
    /// <returns>The combined filter</returns>
    /// <example>
    /// <code>
    /// var builder = Builders&lt;SimpleObject&gt;.CollectionFilter;
    /// var filter = builder.And(builder.Eq(so =&gt; so.PropertyOne, "value"), builder.Eq(so =&gt; so.PropertyTwo, "value2"));
    /// </code>
    /// </example>
    /// <remarks>
    /// This method is equivalent to using the <see cref="Filter{T}.op_BitwiseAnd"/> operator.
    /// </remarks>
    public TFilter And(params TFilter[] filters)
        => Make(LogicalOperator.And, filters);

    /// <summary>
    /// Logical OR operator for combining multiple filters.
    /// </summary>
    /// <param name="filters">Array of filters to combine</param>
    /// <returns>The combined filter</returns>
    /// <example>
    /// <code>
    /// var builder = Builders&lt;SimpleObject&gt;.CollectionFilter;
    /// var filter = builder.Or(builder.Eq(so =&gt; so.PropertyOne, "value"), builder.Eq(so =&gt; so.PropertyTwo, "value2"));
    /// </code>
    /// </example>
    /// <remarks>
    /// This method is equivalent to using the <see cref="Filter{T}.op_BitwiseOr"/> operator.
    /// </remarks>
    public TFilter Or(params TFilter[] filters)
        => Make(LogicalOperator.Or, filters);

    /// <summary>
    /// Logical NOT operator for negating a filter.
    /// </summary>
    /// <param name="filter">The filter to negate</param>
    /// <returns>The negated filter</returns>
    /// <example>
    /// <code>
    /// var builder = Builders&lt;SimpleObject&gt;.CollectionFilter;
    /// var filter = builder.Not(builder.Eq(so =&gt; so.PropertyOne, "value"));
    /// </code>
    /// </example>
    /// <remarks>
    /// This method is equivalent to using the <see cref="Filter{T}.op_LogicalNot"/> operator.
    /// </remarks>
    public TFilter Not(TFilter filter)
        => Make(LogicalOperator.Not, filter);

    // ── Comparison ───────────────────────────────────────────────────────────

    /// <summary>
    /// Greater than operator -- Matches items where the specified field's value is greater than the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Gt{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Gt(string fieldName, object value)
        => MakeOp(fieldName, FilterOperator.GreaterThan, value);

    /// <summary>
    /// Greater than operator -- Matches items where the specified field's value is greater than the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public TFilter Gt<TField>(Expression<Func<T, TField>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.GreaterThan, value);

    /// <summary>
    /// Greater than or equal to operator -- Matches items where the specified field's value is greater than or equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Gte{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Gte(string fieldName, object value)
        => MakeOp(fieldName, FilterOperator.GreaterThanOrEqualTo, value);

    /// <summary>
    /// Greater than or equal to operator -- Matches items where the specified field's value is greater than or equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public TFilter Gte<TField>(Expression<Func<T, TField>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.GreaterThanOrEqualTo, value);

    /// <summary>
    /// Less than operator -- Matches items where the specified field's value is less than the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Lt{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Lt(string fieldName, object value)
        => MakeOp(fieldName, FilterOperator.LessThan, value);

    /// <summary>
    /// Less than operator -- Matches items where the specified field's value is less than the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public TFilter Lt<TField>(Expression<Func<T, TField>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.LessThan, value);

    /// <summary>
    /// Less than or equal to operator -- Matches items where the specified field's value is less than or equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Lte{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Lte(string fieldName, object value)
        => MakeOp(fieldName, FilterOperator.LessThanOrEqualTo, value);

    /// <summary>
    /// Less than or equal to operator -- Matches items where the specified field's value is less than or equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public TFilter Lte<TField>(Expression<Func<T, TField>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.LessThanOrEqualTo, value);

    /// <summary>
    /// Equal to operator -- Matches items where the specified field's value is equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Eq{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Eq(string fieldName, object value)
        => MakeOp(fieldName, FilterOperator.EqualsTo, value);

    /// <summary>
    /// Equal to operator -- Matches items where the specified field's value is equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public TFilter Eq<TField>(Expression<Func<T, TField>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.EqualsTo, value);

    /// <summary>
    /// Not equal to operator -- Matches items where the specified field's value is not equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Ne{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Ne(string fieldName, object value)
        => MakeOp(fieldName, FilterOperator.NotEqualsTo, value);

    /// <summary>
    /// Not equal to operator -- Matches items where the specified field's value is not equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public TFilter Ne<TField>(Expression<Func<T, TField>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.NotEqualsTo, value);

    // ── In / Nin ─────────────────────────────────────────────────────────────

    /// <summary>
    /// In operator -- Match one or more of an array of specified values.
    /// If the field is an array, this will function as a contains query.
    /// </summary>
    /// <typeparam name="T2">The type of the values in the array to check</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="values">The array of values</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="In{TField}(Expression{Func{T, TField[]}},TField)"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter In<T2>(string fieldName, T2[] values)
        => MakeOp(fieldName, FilterOperator.In, values);

    /// <summary>
    /// In operator -- Match one or more of an array of specified values.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public TFilter In<TField>(Expression<Func<T, TField>> expression, TField[] array)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.In, array);

    /// <summary>
    /// In operator -- Match one or more of an array of specified values where the field itself is an array
    /// (functions as a contains query).
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public TFilter In<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.In, array);

    /// <summary>
    /// In operator -- Match where the specified array field contains the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field for this filter.</param>
    /// <param name="value">The value to check for.</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="In{TField}(Expression{Func{T, TField[]}},TField)"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter In(string fieldName, object value)
        => MakeOp(fieldName, FilterOperator.In, new object[] { value });

    /// <summary>
    /// In operator -- Match where the specified array field contains the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check.</typeparam>
    /// <param name="expression">An expression that represents the field for this filter.</param>
    /// <param name="value">The value to check for.</param>
    /// <returns>The filter</returns>
    public TFilter In<TField>(Expression<Func<T, TField[]>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.In, new object[] { value });

    /// <summary>
    /// In operator -- Match one or more key-value pairs in a dictionary field.
    /// </summary>
    /// <typeparam name="TKey">The type of the dictionary keys</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values</typeparam>
    /// <param name="expression">An expression that represents the dictionary field for this filter</param>
    /// <param name="pairs">Array of key-value pairs as tuples</param>
    /// <returns>The filter</returns>
    public TFilter In<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return MakeOp(expression.GetMemberNameTree(), FilterOperator.In, pairArrays);
    }

    /// <summary>
    /// In operator -- Match one or more key-value pairs in a dictionary field.
    /// </summary>
    /// <typeparam name="TKey">The type of the dictionary keys</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="pairs">Array of key-value pairs as tuples</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the In method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter In<TKey, TValue>(string fieldName, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return MakeOp(fieldName, FilterOperator.In, pairArrays);
    }

    /// <summary>
    /// Not in operator -- Match items where the field does not match any of the specified values.
    /// </summary>
    /// <typeparam name="T2">The type of the values in the array to check</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="values">The array of values</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Nin{TField}(Expression{Func{T, TField}},TField[])"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Nin<T2>(string fieldName, T2[] values)
        => MakeOp(fieldName, FilterOperator.NotIn, values);

    /// <summary>
    /// Not in operator -- Match items where the field does not match any of the specified values.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public TFilter Nin<TField>(Expression<Func<T, TField>> expression, TField[] array)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.NotIn, array);

    /// <summary>
    /// Not in operator -- Match items where the field does not match any of the specified values where the field itself is an array.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public TFilter Nin<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.NotIn, array);

    /// <summary>
    /// Not in operator -- Match items where the array field does not match the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="value">The value to check for</param>
    /// <returns>The filter</returns>
    public TFilter Nin<TField>(Expression<Func<T, TField[]>> expression, TField value)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.NotIn, new object[] { value });

    /// <summary>
    /// Not in operator -- Match items where the array field does not match the specified value.
    /// </summary>
    /// <param name="field">The name of the field for this filter</param>
    /// <param name="value">The value to check for</param>
    /// <returns>The filter</returns>
    public TFilter Nin(string field, object value)
        => MakeOp(field, FilterOperator.NotIn, new object[] { value });

    /// <summary>
    /// Not in operator -- Match documents where a dictionary field does not contain any of the specified pairs.
    /// </summary>
    /// <typeparam name="TKey">The type of the dictionary keys</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values</typeparam>
    /// <param name="expression">An expression that represents the dictionary field for this filter</param>
    /// <param name="pairs">Array of key-value pairs as tuples</param>
    /// <returns>The filter</returns>
    public TFilter Nin<TKey, TValue>(Expression<Func<T, IDictionary<TKey, TValue>>> expression, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return MakeOp(expression.GetMemberNameTree(), FilterOperator.NotIn, pairArrays);
    }

    /// <summary>
    /// Not in operator -- Match documents where a dictionary field does not contain any of the specified pairs.
    /// </summary>
    /// <typeparam name="TKey">The type of the dictionary keys</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="pairs">Array of key-value pairs as tuples</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the Nin method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter Nin<TKey, TValue>(string fieldName, (TKey, TValue)[] pairs)
    {
        var pairArrays = pairs.Select(p => new object[] { p.Item1, p.Item2 }).ToArray();
        return MakeOp(fieldName, FilterOperator.NotIn, pairArrays);
    }

    /// <summary>
    /// All operator -- Matches arrays that contain all elements in the specified array.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="array">The array of values to check against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="All{TField}(Expression{Func{T,TField[]}}, TField[])"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public TFilter All<TField>(string fieldName, TField[] array)
        => MakeOp(fieldName, FilterOperator.All, array);

    /// <summary>
    /// All operator -- Matches arrays that contain all elements in the specified array.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values to check against</param>
    /// <returns>The filter</returns>
    public TFilter All<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.All, array);

}

/// <summary>
/// Builds filter expressions for querying documents or rows in a collection or table.
/// </summary>
/// <typeparam name="T">The type of the document or row being filtered.</typeparam>
public class FilterBuilder<T> : FilterBuilder<T, Filter<T>>
{
    /// <inheritdoc/>
    protected override Filter<T> Make(string name, object value) => new(name, value);
}
