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
public abstract class BaseUpdateOneOptions : CommandOptions 
{
}

/// <summary>
/// Options for updating a single document in a collection.
/// </summary>
public class CollectionUpdateOneOptions<T> : BaseUpdateOneOptions where T : class
{
    /// <summary>
    /// Sort order for determining which document to update when multiple match the filter.
    /// </summary>
    public CollectionSortBuilder<T> Sort { get; set; }
    
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
/// Options for updating a single row in a table.
/// </summary>
public sealed class TableUpdateOneOptions : BaseUpdateOneOptions
{
    internal object ToPayload<T>(Filter<T> filter, UpdateBuilder<T> update) where T : class
    {
        return new 
        {
            filter = filter?.Serialize(),
            update = update?.Serialize(),
        };
    }
}

/// <summary>
/// Options for finding and updating a single document in a collection.
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class CollectionFindOneAndUpdateOptions<T> : CollectionUpdateOneOptions<T> where T : class
{
    /// <summary>
    /// Define the projection to apply on the returned document.
    /// </summary>
    public IProjectionBuilder Projection { get; set; }

    /// <summary>
    /// Whether to return the document before or after the update.
    /// </summary>
    public ReturnDocumentDirective? ReturnDocument { get; set; }

    internal new object ToPayload(Filter<T> filter, UpdateBuilder<T> update)
    {
        return new
        {
            filter = filter?.Serialize() ?? new(),
            update = update?.Serialize() ?? new(),
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value),
            projection = Projection?.Projections?.ToDictionary(x => x.FieldName, x => x.Value) ?? new(),
            options = new
            {
                upsert = Upsert ? true : (bool?)null,
                returnDocument = ReturnDocument.Serialize()
            },
        };
    }
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
