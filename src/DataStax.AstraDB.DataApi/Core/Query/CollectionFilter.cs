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
/// A filter for use with collection operations.
/// Create instances using <see cref="CollectionFilterBuilder{T}"/> via <c>Builders&lt;T&gt;.CollectionFilter</c>.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class CollectionFilter<T> : Filter<T>
{
    internal CollectionFilter(string filterName, object value) : base(filterName, value) { }
    internal CollectionFilter(string filterOperator, string fieldName, object value) : base(filterOperator, fieldName, value) { }

    /// <summary>Logical AND operator for combining collection filters.</summary>
    public static CollectionFilter<T> operator &(CollectionFilter<T> left, CollectionFilter<T> right)
        => new LogicalCollectionFilter<T>(LogicalOperator.And, new[] { left, right });

    /// <summary>Logical OR operator for combining collection filters.</summary>
    public static CollectionFilter<T> operator |(CollectionFilter<T> left, CollectionFilter<T> right)
        => new LogicalCollectionFilter<T>(LogicalOperator.Or, new[] { left, right });

    /// <summary>Logical NOT operator for negating a collection filter.</summary>
    public static CollectionFilter<T> operator !(CollectionFilter<T> notFilter)
        => new LogicalCollectionFilter<T>(LogicalOperator.Not, notFilter);
}

internal class LogicalCollectionFilter<T> : CollectionFilter<T>
{
    internal LogicalCollectionFilter(LogicalOperator op, CollectionFilter<T>[] filters)
        : base(op.ToApiString(), filters) { }

    internal LogicalCollectionFilter(LogicalOperator op, CollectionFilter<T> filter)
        : base(op.ToApiString(), filter) { }
}
