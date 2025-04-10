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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using DataStax.AstraDB.DataApi.Utils;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Use an UpdateBuilder to specify the changes to make to document(s) in an update operation.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class UpdateBuilder<T>
{
    private List<Update<T>> _updates = new();

    internal List<Update<T>> Updates => _updates;

    internal Dictionary<string, object> Serialize()
    {
        var grouped = _updates.GroupBy(u => u.UpdateOperator);
        Dictionary<string, object> result = new();
        foreach (var group in grouped)
        {
            result[group.Key] = group.ToDictionary(k => k.FieldName, v => v.FieldValue);
        }
        return result;
    }

    /// <summary>
    /// Noop, here for mongo compatibility
    /// </summary>
    /// <param name="updates"></param>
    /// <returns></returns>
    public UpdateBuilder<T> Combine(params UpdateBuilder<T>[] updates)
    {
        //noop, here for mongo compatibility
        return this;
    }

    /// <summary>
    /// Set the value of a field to the current date and time.
    /// </summary>
    /// <param name="fieldName">The name of the field to update.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="CurrentDate{TField}(Expression{Func{T, TField}})"/>.
    /// </remarks>
    public UpdateBuilder<T> CurrentDate(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.CurrentDate, fieldName, true));
        return this;
    }

    /// <summary>
    /// Set the value of a field to the current date and time.
    /// </summary>
    /// <typeparam name="TField">The type of the field to update.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> CurrentDate<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.CurrentDate, expression.GetMemberNameTree(), true));
        return this;
    }

    /// <summary>
    /// Increment the value of a field by a specified offset.
    /// </summary>
    /// <param name="fieldName">The name of the field to update.</param>
    /// <param name="offset">The amount to increment the field by.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Increment{TField}(Expression{Func{T, TField}}, TField)"/>.
    /// </remarks>
    public UpdateBuilder<T> Increment(string fieldName, double offset)
    {
        _updates.Add(new Update<T>(UpdateOperator.Increment, fieldName, offset));
        return this;
    }

    /// <summary>
    /// Increment the value of a field by a specified offset.
    /// </summary>
    /// <typeparam name="TField">The type of the field to update.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="offset">The amount to increment the field by.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> Increment<TField>(Expression<Func<T, TField>> expression, TField offset)
    {
        _updates.Add(new Update<T>(UpdateOperator.Increment, expression.GetMemberNameTree(), offset));
        return this;
    }

    /// <summary>
    /// Set the value of a field to whichever is lower, the current value or the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to update.</param>
    /// <param name="value">The potential new value.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Min{TField}(Expression{Func{T, TField}}, TField)"/>.
    /// </remarks>
    public UpdateBuilder<T> Min(string fieldName, double value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Min, fieldName, value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to whichever is lower, the current value or the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to update.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="value">The potential new value.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> Min<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Min, expression.GetMemberNameTree(), value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to whichever is higher, the current value or the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to update.</param>
    /// <param name="value">The potential new value</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Max{TField}(Expression{Func{T, TField}}, TField)"/>.
    /// </remarks>
    public UpdateBuilder<T> Max(string fieldName, double value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Max, fieldName, value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to whichever is higher, the current value or the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to update.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="value">The potential new value.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> Max<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Max, expression.GetMemberNameTree(), value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to the product of the current value and the specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to update.</param>
    /// <param name="value">The value to multiply the field by.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Multiply{TField}(Expression{Func{T, TField}}, TField)"/>.
    /// </remarks>
    public UpdateBuilder<T> Multiply(string fieldName, double value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Multiply, fieldName, value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to the product of the current value and the specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to update.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="value">The value to multiply the field by.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> Multiply<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Multiply, expression.GetMemberNameTree(), value));
        return this;
    }

    /// <summary>
    /// Rename a field in the document.
    /// </summary>
    /// <param name="fieldName">The name of the field to rename.</param>
    /// <param name="newFieldName">The new name for the field.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Rename{TField}(Expression{Func{T, TField}}, Expression{Func{T, TField}})"/>.
    /// </remarks>
    public UpdateBuilder<T> Rename(string fieldName, string newFieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Rename, fieldName, newFieldName));
        return this;
    }

    /// <summary>
    /// Rename a field in the document.
    /// </summary>
    /// <typeparam name="TField">The type of the field to rename.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="newExpression">The expression to use to get the new field name.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> Rename<TField>(Expression<Func<T, TField>> expression, Expression<Func<T, TField>> newExpression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Rename, expression.GetMemberNameTree(), newExpression.GetMemberNameTree()));
        return this;
    }

    /// <summary>
    /// Set the value of a field to a specified value.
    /// </summary>
    /// <param name="fieldName">The name of the field to set.</param>
    /// <param name="value">The value to set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Set{TField}(Expression{Func{T, TField}}, TField)"/>.
    /// </remarks>
    public UpdateBuilder<T> Set(string fieldName, object value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Set, fieldName, value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to a specified value.
    /// </summary>
    /// <typeparam name="TField">The type of the field to set.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="value">The value to set.</param>
    /// <returns>The UpdateBuilder instance</returns>
    public UpdateBuilder<T> Set<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Set, expression.GetMemberNameTree(), value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to a specified value only when inserting a new document.
    /// </summary>
    /// <param name="fieldName">The name of the field to set.</param>
    /// <param name="value">The value to set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="SetOnInsert{TField}(Expression{Func{T, TField}}, TField)"/>.
    /// </remarks>
    public UpdateBuilder<T> SetOnInsert(string fieldName, object value)
    {
        _updates.Add(new Update<T>(UpdateOperator.SetOnInsert, fieldName, value));
        return this;
    }

    /// <summary>
    /// Set the value of a field to a specified value only when inserting a new document.
    /// </summary>
    /// <typeparam name="TField">The type of the field to set.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="value">The value to set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> SetOnInsert<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.SetOnInsert, expression.GetMemberNameTree(), value));
        return this;
    }

    /// <summary>
    /// Remove the current value from a field.
    /// </summary>
    /// <param name="fieldName">The name of the field to unset.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Unset{TField}(Expression{Func{T, TField}})"/>.
    /// </remarks>
    public UpdateBuilder<T> Unset(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Unset, fieldName, null));
        return this;
    }

    /// <summary>
    /// Remove the current value from a field.
    /// </summary>
    /// <typeparam name="TField">The type of the field to unset.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> Unset<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Unset, expression.GetMemberNameTree(), null));
        return this;
    }

    /// <summary>
    /// Add a value to a set based on the field name.
    /// </summary>
    /// <param name="fieldName">The name of the field to add the value to.</param>
    /// <param name="value">The value to add to the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="AddToSet{TField}(Expression{Func{T, IEnumerable{TField}}}, TField)"/>.
    /// If the existing field is not a set, it will be converted to a set in the database, which could cause serialization errors.
    /// </remarks>
    public UpdateBuilder<T> AddToSet(string fieldName, object value)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, fieldName, value));
        return this;
    }

    /// <summary>
    /// Add a value to a set based on the field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field to add the value to.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="value">The value to add to the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> AddToSet<TField>(Expression<Func<T, IEnumerable<TField>>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, expression.GetMemberNameTree(), value));
        return this;
    }

    /// <summary>
    /// Add multiple values to a set based on the field name.
    /// </summary>
    /// <param name="fieldName">The name of the field to add the values to.</param>
    /// <param name="values">The values to add to the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="AddToSetEach{TField}(Expression{Func{T, IEnumerable{TField}}}, IEnumerable{TField})"/>.
    /// If the existing field is not a set, it will be converted to a set in the database, which could cause serialization errors.
    /// </remarks>
    public UpdateBuilder<T> AddToSetEach(string fieldName, IEnumerable<object> values)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, fieldName, new AddToSetValue<object> { Each = values.ToList() }));
        return this;
    }

    /// <summary>
    /// Add multiple values to a set based on the field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field to add the values to.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="values">The values to add to the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// If the existing field is not a set, it will be converted to a set in the database, which could cause serialization errors.
    /// </remarks>
    public UpdateBuilder<T> AddToSetEach<TField>(Expression<Func<T, IEnumerable<TField>>> expression, IEnumerable<TField> values)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, expression.GetMemberNameTree(), new AddToSetValue<TField> { Each = values.ToList() }));
        return this;
    }

    /// <summary>
    /// Remove the first element from a set based on the field name.
    /// </summary>
    /// <param name="fieldName"></param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="PopFirst{TField}(Expression{Func{T, TField}})"/>.
    /// </remarks>
    public UpdateBuilder<T> PopFirst(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, fieldName, -1));
        return this;
    }

    /// <summary>
    /// Remove the first element from a set based on the field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field to remove the first element from.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> PopFirst<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, expression.GetMemberNameTree(), -1));
        return this;
    }

    /// <summary>
    /// Remove the last element from a set based on the field name.
    /// </summary>
    /// <param name="fieldName"></param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="PopLast{TField}(Expression{Func{T, TField}})"/>.
    /// </remarks>
    public UpdateBuilder<T> PopLast(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, fieldName, 1));
        return this;
    }

    /// <summary>
    /// Remove the last element from a set based on the field name.
    /// </summary>
    /// <typeparam name="TField">The type of the field to remove the last element from.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> PopLast<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, expression.GetMemberNameTree(), 1));
        return this;
    }

    /// <summary>
    /// Add a value to a set at a specified position.
    /// </summary>
    /// <param name="fieldName">The name of the field to add the value to.</param>
    /// <param name="value">The value to add to the set.</param>
    /// <param name="position">The position to add the value to. If null, the value will be added to the end of the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="Push{TField}(Expression{Func{T, IEnumerable{TField}}}, TField, int?)"/>.
    /// </remarks>
    public UpdateBuilder<T> Push(string fieldName, object value, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, fieldName, new PushUpdateValue<object> { Each = new List<object> { value }, Position = position }));
        return this;
    }

    /// <summary>
    /// Add a value to a set at a specified position.
    /// </summary>
    /// <typeparam name="TField">The type of the field to add the value to.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="value">The value to add to the set.</param>
    /// <param name="position">The position to add the value to. If null, the value will be added to the end of the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> Push<TField>(Expression<Func<T, IEnumerable<TField>>> expression, TField value, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, expression.GetMemberNameTree(), new PushUpdateValue<TField> { Each = new List<TField> { value }, Position = position }));
        return this;
    }

    /// <summary>
    /// Add multiple values to a set starting at a specified position.
    /// </summary>
    /// <param name="fieldName">The name of the field to add the values to.</param>
    /// <param name="values">The values to add to the set.</param>
    /// <param name="position">The position to add the values to. If null, the values will be added to the end of the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="PushEach{TField}(Expression{Func{T, IEnumerable{TField}}}, IEnumerable{TField}, int?)"/>.
    /// </remarks>
    public UpdateBuilder<T> PushEach(string fieldName, IEnumerable<object> values, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, fieldName, new PushUpdateValue<object> { Each = values.ToList(), Position = position }));
        return this;
    }

    /// <summary>
    /// Add multiple values to a set starting at a specified position.
    /// </summary>
    /// <typeparam name="TField">The type of the field to add the values to.</typeparam>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <param name="values">The values to add to the set.</param>
    /// <param name="position">The position to add the values to. If null, the values will be added to the end of the set.</param>
    /// <returns>The UpdateBuilder instance.</returns>
    public UpdateBuilder<T> PushEach<TField>(Expression<Func<T, IEnumerable<TField>>> expression, IEnumerable<TField> values, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, expression.GetMemberNameTree(), new PushUpdateValue<TField> { Each = values.ToList(), Position = position }));
        return this;
    }

}