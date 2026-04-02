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
}
