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

public class FilterBuilder<T>
{
    /// <summary>
    /// Logical AND operator for combining multiple filters.
    /// </summary>
    /// <param name="filters">Array of filters to combine</param>
    /// <returns>The combined filter</returns>
    /// <example>
    /// <code>
    /// // Find documents where the "field" property equals "value" and "field2" property equals "value2"
    /// var builder = Builders&lt;SimpleObject&gt;.Filter;
    /// var filter = builder.And(builder.Eq(so =&gt; so.Properties.PropertyOne, "value"), builder.Eq(so =&gt; so.Properties.PropertyTwo, "value2"));
    /// </code>
    /// </example>
    /// <remarks>
    /// This method is equivalent to using the <see cref="Filter{T}.op_BitwiseAnd"/> operator.
    /// </remarks>
    public Filter<T> And(params Filter<T>[] filters)
    {
        return new LogicalFilter<T>(LogicalOperator.And, filters);
    }

    /// <summary>
    /// Logical OR operator for combining multiple filters.
    /// </summary>
    /// <param name="filters">Array of filters to combine</param>
    /// <returns>The combined filter</returns>
    /// <example>
    /// <code>
    /// // Find documents where the "field" property equals "value" or "field2" property equals "value2"
    /// var builder = Builders&lt;SimpleObject&gt;.Filter;
    /// var filter = builder.Or(builder.Eq(so =&gt; so.Properties.PropertyOne, "value"), builder.Eq(so =&gt; so.Properties.PropertyTwo, "value2"));
    /// </code>
    /// </example>
    /// <remarks>
    /// This method is equivalent to using the <see cref="Filter{T}.op_BitwiseOr"/> operator.
    /// </remarks>
    public Filter<T> Or(params Filter<T>[] filters)
    {
        return new LogicalFilter<T>(LogicalOperator.Or, filters);
    }

    /// <summary>
    /// Logical NOT operator for negating a filter.
    /// </summary>
    /// <param name="filter">The filter to negate</param>
    /// <returns>The negated filter</returns>
    /// <example>
    /// <code>
    /// // Find documents where the "field" property does not equal "value"
    /// var builder = Builders&lt;SimpleObject&gt;.Filter;
    /// var filter = builder.Not(builder.Eq(so =&gt; so.Properties.PropertyOne, "value"));
    /// </code>
    /// </example>
    /// <remarks>
    /// This method is equivalent to using the <see cref="Filter{T}.op_LogicalNot"/> operator.
    /// </remarks>
    public Filter<T> Not(Filter<T> filter)
    {
        return new LogicalFilter<T>(LogicalOperator.Not, filter);
    }

    /// <summary>
    /// Greater than operator -- Matches documents where the specified field's value is greater than the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Gt{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Gt(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.GreaterThan, value);
    }


    /// <summary>
    /// Greater than operator -- Matches documents where the specified field's value is greater than the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public Filter<T> Gt<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThan, value);
    }

    /// <summary>
    /// Greater than or equal to operator -- Matches documents where the specified field's value is greater than or equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Gte{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Gte(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.GreaterThanOrEqualTo, value);
    }

    /// <summary>
    /// Greater than or equal to operator -- Matches documents where the specified field's value is greater than or equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public Filter<T> Gte<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThanOrEqualTo, value);
    }

    /// <summary>
    /// Less than operator -- Matches documents where the specified field's value is less than the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Lt{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Lt(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.LessThan, value);
    }

    /// <summary>
    /// Less than operator -- Matches documents where the specified field's value is less than the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public Filter<T> Lt<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.LessThan, value);
    }

    /// <summary>
    /// Less than or equal to operator -- Matches documents where the specified field's value is less than or equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Lte{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Lte(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.LessThanOrEqualTo, value);
    }

    /// <summary>
    /// Less than or equal to operator -- Matches documents where the specified field's value is less than or equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public Filter<T> Lte<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.LessThanOrEqualTo, value);
    }

    /// <summary>
    /// Equal to operator -- Matches documents where the specified field's value is equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Eq{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>  
    public Filter<T> Eq(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.EqualsTo, value);
    }

    /// <summary>
    /// Equal to operator -- Matches documents where the specified field's value is equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public Filter<T> Eq<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.EqualsTo, value);
    }

    /// <summary>
    /// Not equal to operator -- Matches documents where the specified field's value is not equal to the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Ne{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Ne(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.NotEqualsTo, value);
    }

    /// <summary>
    /// Not equal to operator -- Matches documents where the specified field's value is not equal to the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to compare</typeparam>
    /// <param name="expression">An expression that represents the field to compare</param>
    /// <param name="value">The value to compare against</param>
    /// <returns>The filter</returns>
    public Filter<T> Ne<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.NotEqualsTo, value);
    }

    /// <summary>
    /// In operator -- Match one or more of an array of specified values.
    /// 
    /// If the field is an array, this will function as a contains query.
    /// </summary>
    /// <typeparam name="T2">The type of the values in the array to check</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="values">The array of values</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="In{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> In<T2>(string fieldName, T2[] values)
    {
        return new Filter<T>(fieldName, FilterOperator.In, values);
    }

    /// <summary>
    /// In operator -- Match one or more of an array of specified values.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public Filter<T> In<TField>(Expression<Func<T, TField>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);
    }

    /// <summary>
    /// In operator -- Match one or more of an array of specified values where the field itself is an array
    /// (functions as a contains query)
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public Filter<T> In<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);
    }

    /// <summary>
    /// Not in operator -- Match documents where the field does not match any of the specified values.
    /// </summary>
    /// <typeparam name="T2">The type of the values in the array to check</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="values">The array of values</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Nin{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Nin<T2>(string fieldName, T2[] values)
    {
        return new Filter<T>(fieldName, FilterOperator.NotIn, values);
    }

    /// <summary>
    /// Not in operator -- Match documents where the field does not match any of the specified values.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public Filter<T> Nin<TField>(Expression<Func<T, TField>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);
    }

    /// <summary>
    /// Not in operator -- Match documents where the field does not match any of the specified values where the field itself is an array.
    /// </summary>
    /// <typeparam name="TField">The type of the field to check</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values</param>
    /// <returns>The filter</returns>
    public Filter<T> Nin<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);
    }

    /// <summary>
    /// Exists operator -- Match documents where the field exists.
    /// </summary>
    /// <param name="fieldName">The name of the field to check for</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Exists{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Exists(string fieldName)
    {
        return new Filter<T>(fieldName, FilterOperator.Exists, true);
    }

    /// <summary>
    /// Exists operator -- Match documents where the field exists.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="expression">An expression that represents the field to check for</param>
    /// <returns>The filter</returns>
    public Filter<T> Exists<TField>(Expression<Func<T, IEnumerable<TField>>> expression)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Exists, true);
    }

    /// <summary>
    /// All operator -- Matches arrays that contain all elements in the specified array.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="array">The array of values to check against</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="All{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> All<TField>(string fieldName, TField[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.All, array);
    }

    /// <summary>
    /// All operator -- Matches arrays that contain all elements in the specified array.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="array">The array of values to check against</param>
    /// <returns>The filter</returns>
    public Filter<T> All<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.All, array);
    }

    /// <summary>
    /// Size operator -- Matches documents where the specified array has the specified size.
    /// </summary>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="size">The size of the array to match</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Size{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public Filter<T> Size(string fieldName, int size)
    {
        return new Filter<T>(fieldName, FilterOperator.Size, size);
    }

    /// <summary>
    /// Size operator -- Matches documents where the specified array has the specified size.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="size">The size of the array to match</param>
    /// <returns>The filter</returns>
    public Filter<T> Size<TField>(Expression<Func<T, TField[]>> expression, int size)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Size, size);
    }
}