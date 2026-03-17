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

/// <summary>
/// A filter for use with table operations.
/// Create instances using <see cref="TableFilterBuilder{T}"/> via <c>Builders&lt;T&gt;.TableFilter</c>.
/// </summary>
/// <typeparam name="T">The type of the rows in the table.</typeparam>
public class TableFilter<T> : Filter<T>
{
    internal TableFilter(string filterName, object value) : base(filterName, value) { }
    internal TableFilter(string filterOperator, string fieldName, object value) : base(filterOperator, fieldName, value) { }

    /// <summary>Logical AND operator for combining table filters.</summary>
    public static TableFilter<T> operator &(TableFilter<T> left, TableFilter<T> right)
        => new LogicalTableFilter<T>(LogicalOperator.And, new[] { left, right });

    /// <summary>Logical OR operator for combining table filters.</summary>
    public static TableFilter<T> operator |(TableFilter<T> left, TableFilter<T> right)
        => new LogicalTableFilter<T>(LogicalOperator.Or, new[] { left, right });

    /// <summary>Logical NOT operator for negating a table filter.</summary>
    public static TableFilter<T> operator !(TableFilter<T> notFilter)
        => new LogicalTableFilter<T>(LogicalOperator.Not, notFilter);
}

internal class LogicalTableFilter<T> : TableFilter<T>
{
    internal LogicalTableFilter(LogicalOperator op, TableFilter<T>[] filters)
        : base(op.ToApiString(), filters) { }

    internal LogicalTableFilter(LogicalOperator op, TableFilter<T> filter)
        : base(op.ToApiString(), filter) { }
}
