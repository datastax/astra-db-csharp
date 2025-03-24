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
    public Filter<T> And(params Filter<T>[] filters)
    {
        return new LogicalFilter<T>(LogicalOperator.And, filters);
    }

    public Filter<T> Or(params Filter<T>[] filters)
    {
        return new LogicalFilter<T>(LogicalOperator.Or, filters);
    }

    public Filter<T> Not(Filter<T> filter)
    {
        return new LogicalFilter<T>(LogicalOperator.Not, filter);
    }

    public Filter<T> Gt(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.GreaterThan, value);
    }

    public Filter<T> Gt<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThan, value);
    }

    public Filter<T> Gte(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.GreaterThanOrEqualTo, value);
    }

    public Filter<T> Gte<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.GreaterThanOrEqualTo, value);
    }

    public Filter<T> Lt(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.LessThan, value);
    }

    public Filter<T> Lt<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.LessThan, value);
    }

    public Filter<T> Lte(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.LessThanOrEqualTo, value);
    }

    public Filter<T> Lte<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.LessThanOrEqualTo, value);
    }

    public Filter<T> Eq(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.EqualsTo, value);
    }

    public Filter<T> Eq<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.EqualsTo, value);
    }

    public Filter<T> Ne(string fieldName, object value)
    {
        return new Filter<T>(fieldName, FilterOperator.NotEqualsTo, value);
    }

    public Filter<T> Ne<TField>(Expression<Func<T, TField>> expression, TField value)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.NotEqualsTo, value);
    }

    public Filter<T> In<T2>(string fieldName, T2[] values)
    {
        return new Filter<T>(fieldName, FilterOperator.In, values);
    }

    public Filter<T> In<TField>(Expression<Func<T, TField>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);
    }

    public Filter<T> In<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.In, array);
    }

    public Filter<T> Nin<T2>(string fieldName, T2[] values)
    {
        return new Filter<T>(fieldName, FilterOperator.NotIn, values);
    }

    public Filter<T> Nin<TField>(Expression<Func<T, TField>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);
    }

    public Filter<T> Nin<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.NotIn, array);
    }

    public Filter<T> Exists(string fieldName)
    {
        return new Filter<T>(fieldName, FilterOperator.Exists, true);
    }

    public Filter<T> Exists<TField>(Expression<Func<T, IEnumerable<TField>>> expression)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Exists, true);
    }

    public Filter<T> All<TField>(string fieldName, TField[] array)
    {
        return new Filter<T>(fieldName, FilterOperator.All, array);
    }

    public Filter<T> All<TField>(Expression<Func<T, TField[]>> expression, TField[] array)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.All, array);
    }

    public Filter<T> Size(string fieldName, int size)
    {
        return new Filter<T>(fieldName, FilterOperator.Size, size);
    }

    public Filter<T> Size<TField>(Expression<Func<T, TField[]>> expression, int size)
    {
        return new Filter<T>(expression.GetMemberNameTree(), FilterOperator.Size, size);
    }
}