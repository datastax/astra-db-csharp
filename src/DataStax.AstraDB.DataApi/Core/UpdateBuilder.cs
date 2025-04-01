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
using System.Text.Json.Serialization;
using DataStax.AstraDB.DataApi.Utils;

namespace DataStax.AstraDB.DataApi.Core;

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

    public UpdateBuilder<T> Combine(params UpdateBuilder<T>[] updates)
    {
        //noop, here for mongo compatibility
        return this;
    }

    public UpdateBuilder<T> CurrentDate(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.CurrentDate, fieldName, true));
        return this;
    }

    public UpdateBuilder<T> CurrentDate<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.CurrentDate, expression.GetMemberNameTree(), true));
        return this;
    }

    public UpdateBuilder<T> Increment(string fieldName, double offset)
    {
        _updates.Add(new Update<T>(UpdateOperator.Increment, fieldName, offset));
        return this;
    }

    public UpdateBuilder<T> Increment<TField>(Expression<Func<T, TField>> expression, TField offset)
    {
        _updates.Add(new Update<T>(UpdateOperator.Increment, expression.GetMemberNameTree(), offset));
        return this;
    }

    public UpdateBuilder<T> Min(string fieldName, double value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Min, fieldName, value));
        return this;
    }

    public UpdateBuilder<T> Min<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Min, expression.GetMemberNameTree(), value));
        return this;
    }

    public UpdateBuilder<T> Max(string fieldName, double value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Max, fieldName, value));
        return this;
    }

    public UpdateBuilder<T> Max<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Max, expression.GetMemberNameTree(), value));
        return this;
    }

    public UpdateBuilder<T> Multiply(string fieldName, double value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Multiply, fieldName, value));
        return this;
    }

    public UpdateBuilder<T> Multiply<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Multiply, expression.GetMemberNameTree(), value));
        return this;
    }

    public UpdateBuilder<T> Rename(string fieldName, string newFieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Rename, fieldName, newFieldName));
        return this;
    }

    public UpdateBuilder<T> Rename<TField>(Expression<Func<T, TField>> expression, Expression<Func<T, TField>> newExpression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Rename, expression.GetMemberNameTree(), newExpression.GetMemberNameTree()));
        return this;
    }

    public UpdateBuilder<T> Set(string fieldName, object value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Set, fieldName, value));
        return this;
    }

    public UpdateBuilder<T> Set<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.Set, expression.GetMemberNameTree(), value));
        return this;
    }

    public UpdateBuilder<T> SetOnInsert(string fieldName, object value)
    {
        _updates.Add(new Update<T>(UpdateOperator.SetOnInsert, fieldName, value));
        return this;
    }

    public UpdateBuilder<T> SetOnInsert<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.SetOnInsert, expression.GetMemberNameTree(), value));
        return this;
    }

    public UpdateBuilder<T> Unset(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Unset, fieldName, null));
        return this;
    }

    public UpdateBuilder<T> Unset<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Unset, expression.GetMemberNameTree(), null));
        return this;
    }

    public UpdateBuilder<T> AddToSet(string fieldName, object value)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, fieldName, value));
        return this;
    }

    public UpdateBuilder<T> AddToSet<TField>(Expression<Func<T, IEnumerable<TField>>> expression, TField value)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, expression.GetMemberNameTree(), value));
        return this;
    }

    public UpdateBuilder<T> AddToSetEach(string fieldName, IEnumerable<object> values)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, fieldName, new AddToSetValue<object> { Each = values.ToList() }));
        return this;
    }

    public UpdateBuilder<T> AddToSetEach<TField>(Expression<Func<T, TField>> expression, IEnumerable<TField> values)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, expression.GetMemberNameTree(), new AddToSetValue<TField> { Each = values.ToList() }));
        return this;
    }

    public UpdateBuilder<T> AddToSetEach<TField>(Expression<Func<T, IEnumerable<TField>>> expression, IEnumerable<TField> values)
    {
        _updates.Add(new Update<T>(UpdateOperator.AddToSet, expression.GetMemberNameTree(), new AddToSetValue<TField> { Each = values.ToList() }));
        return this;
    }

    public UpdateBuilder<T> PopFirst(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, fieldName, -1));
        return this;
    }

    public UpdateBuilder<T> PopFirst<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, expression.GetMemberNameTree(), -1));
        return this;
    }

    public UpdateBuilder<T> PopLast(string fieldName)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, fieldName, 1));
        return this;
    }

    public UpdateBuilder<T> PopLast<TField>(Expression<Func<T, TField>> expression)
    {
        _updates.Add(new Update<T>(UpdateOperator.Pop, expression.GetMemberNameTree(), 1));
        return this;
    }

    public UpdateBuilder<T> Push(string fieldName, object value, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, fieldName, new PushUpdateValue<object> { Each = new List<object> { value }, Position = position }));
        return this;
    }

    public UpdateBuilder<T> Push<TField>(Expression<Func<T, IEnumerable<TField>>> expression, TField value, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, expression.GetMemberNameTree(), new PushUpdateValue<TField> { Each = new List<TField> { value }, Position = position }));
        return this;
    }

    public UpdateBuilder<T> PushEach(string fieldName, IEnumerable<object> values, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, fieldName, new PushUpdateValue<object> { Each = values.ToList(), Position = position }));
        return this;
    }

    public UpdateBuilder<T> PushEach<TField>(Expression<Func<T, TField>> expression, IEnumerable<TField> values, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, expression.GetMemberNameTree(), new PushUpdateValue<TField> { Each = values.ToList(), Position = position }));
        return this;
    }

    public UpdateBuilder<T> PushEach<TField>(Expression<Func<T, IEnumerable<TField>>> expression, IEnumerable<TField> values, int? position = null)
    {
        _updates.Add(new Update<T>(UpdateOperator.Push, expression.GetMemberNameTree(), new PushUpdateValue<TField> { Each = values.ToList(), Position = position }));
        return this;
    }

    //TODO: add Pull / Xor, etc from Mongo?
}