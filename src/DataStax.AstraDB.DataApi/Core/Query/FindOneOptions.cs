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

using System.Linq;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Base class for find-one operation options.
/// </summary>
public abstract class BaseFindOneOptions<T, TSort> : CommandOptions 
    where T : class
    where TSort : SortBuilder<T>
{
    /// <summary>
    /// Sort order for the query.
    /// </summary>
    public TSort Sort { get; set; }
    
    /// <summary>
    /// Projection to apply to the results.
    /// </summary>
    public IProjectionBuilder Projection { get; set; }
    
    /// <summary>
    /// Whether to include similarity scores in the results.
    /// </summary>
    public bool? IncludeSimilarity { get; set; }

    internal object ToPayload(Filter<T> filter)
    {
        return new
        {
            filter = filter?.Serialize(),
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value),
            projection = Projection?.Projections?.ToDictionary(x => x.FieldName, x => x.Value) ?? new(),
            options = new { includeSimilarity = IncludeSimilarity },
        };
    }
}

/// <summary>
/// Options for finding a single document in a collection.
/// </summary>
public sealed class CollectionFindOneOptions<T> : BaseFindOneOptions<T, CollectionSortBuilder<T>> where T : class
{
}

/// <summary>
/// Options for finding a single row in a table.
/// </summary>
public sealed class TableFindOneOptions<T> : BaseFindOneOptions<T, TableSortBuilder<T>> where T : class
{
}
