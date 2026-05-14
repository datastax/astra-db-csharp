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
/// Base class for update-one operation options.
/// </summary>
public abstract class BaseUpdateOneOptions<T, TSort> : CommandOptions 
    where T : class
    where TSort : SortBuilder<T>
{
    /// <summary>
    /// Sort order for determining which document to update when multiple match the filter.
    /// </summary>
    public TSort Sort { get; set; }
    
    /// <summary>
    /// Whether to insert a new document if the filter does not match any documents.
    /// </summary>
    public bool Upsert { get; set; }

    internal object ToPayload(Filter<T> filter, UpdateBuilder<T> update)
    {
        return new
        {
            filter = filter?.Serialize(),
            update = update?.Serialize(),
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value),
            options = new { upsert = Upsert }
        };
    }
}

/// <summary>
/// Options for updating a single document in a collection.
/// </summary>
public sealed class CollectionUpdateOneOptions<T> : BaseUpdateOneOptions<T, CollectionSortBuilder<T>> where T : class
{
}

/// <summary>
/// Options for updating a single row in a table.
/// </summary>
public sealed class TableUpdateOneOptions<T> : BaseUpdateOneOptions<T, TableSortBuilder<T>> where T : class
{
}

/// <summary>
/// Options for updating multiple documents in a collection.
/// </summary>
public sealed class CollectionUpdateManyOptions : CommandOptions
{
    /// <summary>
    /// Whether to insert a new document if the filter does not match any documents.
    /// </summary>
    public bool Upsert { get; set; }
    
    internal object ToPayload<T>(Filter<T> filter, UpdateBuilder<T> update, string pageState) where T : class
    {
        return new
        {
            filter = filter?.Serialize(),
            update = update?.Serialize(),
            options = new
            {
                upsert = Upsert, 
                pageState,
            },
        };
    }
}
