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
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A builder for creating filter definitions for collection queries.
/// Obtain an instance via <c>Builders&lt;T&gt;.CollectionFilter</c>.
/// </summary>
/// <typeparam name="T">The type of the documents in the collection.</typeparam>
public class CollectionFilterBuilder<T> : FilterBuilder<T, CollectionFilter<T>>
{
    /// <inheritdoc/>
    protected override CollectionFilter<T> Make(string name, object value) => new(name, value);

    /// <summary>
    /// Lexical match operator -- Matches documents where the document's lexical field value is a
    /// lexicographical match to the specified string of space-separated keywords or terms.
    /// </summary>
    public CollectionFilter<T> LexicalMatch(string value)
        => new(DataApiKeywords.Lexical, FilterOperator.Match, value);

    /// <summary>
    /// Size operator -- Matches items where the specified array has the specified size.
    /// </summary>
    /// <param name="fieldName">The name of the field for this filter</param>
    /// <param name="size">The size of the array to match</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Size{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public CollectionFilter<T> Size(string fieldName, int size)
        => MakeOp(fieldName, FilterOperator.Size, size);

    /// <summary>
    /// Size operator -- Matches items where the specified array has the specified size.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="expression">An expression that represents the field for this filter</param>
    /// <param name="size">The size of the array to match</param>
    /// <returns>The filter</returns>
    public CollectionFilter<T> Size<TField>(Expression<Func<T, TField[]>> expression, int size)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.Size, size);


    /// <summary>
    /// Exists operator -- Match items where the field exists.
    /// </summary>
    /// <param name="fieldName">The name of the field to check for</param>
    /// <returns>The filter</returns>
    /// <remarks>
    /// We recommend using the <see cref="Exists{TField}"/> method with expressions instead of strings for clarity and type safety.
    /// </remarks>
    public CollectionFilter<T> Exists(string fieldName)
        => MakeOp(fieldName, FilterOperator.Exists, true);

    /// <summary>
    /// Exists operator -- Match items where the field exists.
    /// </summary>
    /// <typeparam name="TField">The type of the field</typeparam>
    /// <param name="expression">An expression that represents the field to check for</param>
    /// <returns>The filter</returns>
    public CollectionFilter<T> Exists<TField>(Expression<Func<T, IEnumerable<TField>>> expression)
        => MakeOp(expression.GetMemberNameTree(), FilterOperator.Exists, true);
}
