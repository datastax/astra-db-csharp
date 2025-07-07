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
/// Represents options for find operations that return multiple results.
/// </summary>
/// <typeparam name="T">The type of the document/entity.</typeparam>
/// <typeparam name="TSort">The type of the sort builder.</typeparam>
internal interface IFindManyOptions<T, TSort> : IFindOptions<T, TSort>
    where TSort : SortBuilder<T>
{
    /// <summary>
    /// Gets or sets the number of documents to skip.
    /// </summary>
    internal int? Skip { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of documents to return.
    /// </summary>
    internal int? Limit { get; set; }

    /// <summary>
    /// Gets or sets whether to include the sort vector in the results.
    /// </summary>
    internal bool? IncludeSortVector { get; set; }

    internal IFindManyOptions<T, TSort> Clone();
}
