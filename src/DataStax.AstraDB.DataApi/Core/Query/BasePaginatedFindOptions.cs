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

using System.Collections.Generic;
using System.Linq;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Abstract class for (formally paginated) find-like operation options,
/// capturing the common traits between ordinary find and find-and-rerank.
/// </summary>
public abstract class BasePaginatedFindOptions<T, TOptions> : CommandOptions
    where T : class
    where TOptions : BasePaginatedFindOptions<T, TOptions>
{
    /// <summary>
    /// Projection to apply to the results.
    /// </summary>
    public IProjectionBuilder Projection { get; set; }
    
    /// <summary>
    /// Whether to include the sort vector in the results.
    /// </summary>
    public bool? IncludeSortVector { get; set; }

    /// <summary>
    /// The initial page state used to resume pagination from a previous find-many operation.
    /// </summary>
    public string InitialPageState { get; set; }

    /// <summary>
    /// The maximum number of documents to return.
    /// </summary>
    public int? Limit { get; set; }

    internal abstract object ToPayload(Filter<T> filter, string pageState = null);

    internal abstract TOptions ShallowClone();
}
