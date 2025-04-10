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

using DataStax.AstraDB.DataApi.Core.Query;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// A collection of builders for creating filter, projection, sort, and update definitions
/// </summary>
/// <typeparam name="T">The type of the documents</typeparam>
public class Builders<T>
{
    /// <summary>
    /// A builder for creating filter definitions
    /// </summary>
    /// <example>
    /// <code>
    /// var filter = Builders&lt;DocumentType&gt;.Filter;
    /// filter = filter.Where(x => x.Name == "NameSearch");
    /// </code>
    /// </example>
    public static FilterBuilder<T> Filter => new();
    /// <summary>
    /// A builder for creating projection definitions
    /// </summary>
    /// <example>
    /// <code>
    /// var projection = Builders&lt;DocumentType&gt;.Projection;
    /// projection = projection.Include(x => x.Name);
    /// </code>
    /// </example>
    public static ProjectionBuilder<T> Projection => new();
    /// <summary>
    /// A builder for creating sort definitions
    /// </summary>
    /// <example>
    /// <code>
    /// var sort = Builders&lt;DocumentType&gt;.Sort;
    /// sort = sort.Ascending(x => x.Name);
    /// </code>
    /// </example>
    public static SortBuilder<T> Sort => new();
    /// <summary>
    /// A builder for creating update definitions
    /// </summary>
    /// <example>
    /// <code>
    /// var update = Builders&lt;DocumentType&gt;.Update;
    /// update = update.Set(x => x.Name, "NewName");
    /// </code>
    /// </example>
    public static UpdateBuilder<T> Update => new();
}