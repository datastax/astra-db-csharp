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
/// Base class for find-many operation options.
/// </summary>
public abstract class BaseFindOptions<T, TSort> : BasePaginatedFindOptions<T, BaseFindOptions<T, TSort>>
    where T : class
    where TSort : SortBuilder<T>
{
    /// <summary>
    /// Sort order for the query.
    /// </summary>
    public TSort Sort { get; set; }
    
    /// <summary>
    /// Whether to include similarity scores in the results.
    /// </summary>
    public bool? IncludeSimilarity { get; set; }
    
    /// <summary>
    /// The number of documents to skip before starting to return documents.
    /// </summary>
    public int? Skip { get; set; }

    internal override object ToPayload(Filter<T> filter, string pageState = null)
    {
        var options = new Dictionary<string, object>();
        if (IncludeSimilarity.HasValue)
            options["includeSimilarity"] = IncludeSimilarity.Value;
        if (IncludeSortVector.HasValue)
            options["includeSortVector"] = IncludeSortVector.Value;
        if (!string.IsNullOrEmpty(pageState ?? InitialPageState))
            options["pageState"] = pageState ?? InitialPageState;
        if (Skip.HasValue)
            options["skip"] = Skip.Value;
        if (Limit.HasValue)
            options["limit"] = Limit.Value;

        return new
        {
            filter = filter?.Serialize(),
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value),
            projection = Projection?.Projections?.ToDictionary(x => x.FieldName, x => x.Value) ?? new(),
            options = options,
        };
    }
    
    internal override BaseFindOptions<T, TSort> ShallowClone()
    {
        return (BaseFindOptions<T, TSort>)MemberwiseClone();
    }
}

/// <summary>
/// Options for finding multiple documents in a collection.
/// </summary>
public sealed class CollectionFindOptions<T> : BaseFindOptions<T, CollectionSortBuilder<T>> where T : class
{
}

/// <summary>
/// Options for finding multiple rows in a table.
/// </summary>
public sealed class TableFindOptions<T> : BaseFindOptions<T, TableSortBuilder<T>> where T : class
{
}
