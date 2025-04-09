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

using MongoDB.Bson;
using System.Collections.Generic;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Filters are used target specific documents in a collection. This class is not used directly,
/// you can create filters using the <see cref="FilterBuilder{T}"/> class.
/// </summary>
/// <typeparam name="T">Type of document in the collection</typeparam>
public class Filter<T>
{
    internal virtual object Name { get; }
    internal virtual object Value { get; }

    internal Filter(string filterName, object value)
    {
        Name = filterName;
        Value = value;
    }

    internal Filter(string filterOperator, string fieldName, object value)
    {
        Name = filterOperator;
        Value = new Filter<T>(fieldName, value);
    }

    /// <summary>
    /// Logical AND operator for combining filters.
    /// </summary>
    /// <param name="left">First filter to combine</param>
    /// <param name="right">Second filter to combine</param>
    /// <returns>The combined filter</returns>
    /// <example>
    /// <code>
    /// // Find documents where the "field" property equals "value" and "field2" property equals "value2"
    /// var filter = Filter.Eq("field", "value") &amp; Filter.Eq("field2", "value2");
    /// </code>
    /// </example>
    public static Filter<T> operator &(Filter<T> left, Filter<T> right)
    {
        return new LogicalFilter<T>(LogicalOperator.And, new[] { left, right });
    }

    /// <summary>
    /// Logical OR operator for combining filters.
    /// </summary>
    /// <param name="left">First filter to combine</param>
    /// <param name="right">Second filter to combine</param>
    /// <returns>The combined filter</returns>
    /// <example>
    /// <code>
    /// // Find documents where the "field" property equals either "value" or "value2"
    /// var filter = Filter.Eq("field", "value") | Filter.Eq("field", "value2");
    /// </code>
    /// </example>
    public static Filter<T> operator |(Filter<T> left, Filter<T> right)
    {
        return new LogicalFilter<T>(LogicalOperator.Or, new[] { left, right });
    }

    /// <summary>
    /// Logical NOT operator for negating a filter.
    /// </summary>
    /// <param name="notFilter">The filter to negate</param>
    /// <returns>The negated filter</returns>
    /// <example>
    /// <code>
    /// // Find documents where the "field" property does not equal "value"
    /// var filter = !Filter.Eq("field", "value");
    /// </code>
    /// </example>
    public static Filter<T> operator !(Filter<T> notFilter)
    {
        return new LogicalFilter<T>(LogicalOperator.Not, notFilter);
    }

}

internal static class FilterExtensions
{
    internal static Dictionary<string, object> Serialize<T>(this Filter<T> filter)
    {
        var result = new Dictionary<string, object>();
        if (filter.Value is Filter<T>[] filtersArray)
        {
            var serializedArray = new List<object>();
            foreach (var nestedFilter in filtersArray)
            {
                serializedArray.Add(nestedFilter.Serialize());
            }
            result[filter.Name.ToString()] = serializedArray;
        }
        else
        {
            //TODO: abstract out ObjectId handling
            result[filter.Name.ToString()] = filter.Value is Filter<T> nestedFilter ? nestedFilter.Serialize() :
              filter.Value is ObjectId ? filter.Value.ToString() : filter.Value;
        }
        return result;
    }
}