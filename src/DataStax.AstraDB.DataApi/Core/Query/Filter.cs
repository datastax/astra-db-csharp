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

namespace DataStax.AstraDB.DataApi.Core.Query;

public class Filter<T>
{
    public string Name { get; }
    public object Value { get; }

    public Filter(string filterName, object value)
    {
        Name = filterName;
        Value = value;
    }

    public Filter(string filterOperator, string fieldName, object value)
    {
        Name = filterOperator;
        Value = new Filter<T>(fieldName, value);
    }

    public static Filter<T> operator &(Filter<T> left, Filter<T> right)
    {
        return new LogicalFilter<T>(LogicalOperator.And, new[] { left, right });
    }

    public static Filter<T> operator |(Filter<T> left, Filter<T> right)
    {
        return new LogicalFilter<T>(LogicalOperator.Or, new[] { left, right });
    }

    public static Filter<T> operator !(Filter<T> notFilter)
    {
        return new LogicalFilter<T>(LogicalOperator.Or, new[] { notFilter });
    }

}