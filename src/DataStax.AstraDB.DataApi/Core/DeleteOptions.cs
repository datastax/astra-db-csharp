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
using System.Linq;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Base class for delete-one operation options.
/// </summary>
public abstract class BaseDeleteOneOptions<T, TSort> : CommandOptions
    where T : class
    where TSort : SortBuilder<T>
{
    /// <summary>
    /// Sort order for determining which document to delete when multiple match the filter.
    /// </summary>
    public TSort Sort { get; set; }

    internal object ToPayload(Filter<T> filter)
    {
        return new
        {
            filter = filter?.Serialize() ?? new(),
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value)
        };
    }
}

/// <summary>
/// Options for deleting a document from a collection.
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class CollectionDeleteOneOptions<T> : BaseDeleteOneOptions<T, CollectionSortBuilder<T>> where T : class
{
}

/// <summary>
/// Options for deleting a row from a table.
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class TableDeleteOneOptions<T> : BaseDeleteOneOptions<T, TableSortBuilder<T>> where T : class
{
}


/// <summary>
/// Base class for delete-many operation options.
/// </summary>
public abstract class BaseDeleteManyOptions : CommandOptions
{
}

/// <summary>
/// Options for deleting multiple documents from a collection.
/// </summary>
public sealed class CollectionDeleteManyOptions : BaseDeleteManyOptions
{
    internal object ToPayload<T>(Filter<T> filter) where T : class
    {
        return new
        {
            filter = filter?.Serialize(),
        };
    }
}

/// <summary>
/// Options for deleting multiple rows from a table.
/// </summary>
public sealed class TableDeleteManyOptions : BaseDeleteManyOptions
{
    internal object ToPayload<T>(Filter<T> filter) where T : class
    {
        return new
        {
            filter = filter?.Serialize(),
        };
    }
}

/// <summary>
/// Options for finding and deleting a single document from a collection.
/// </summary>
/// <typeparam name="T"></typeparam>
public class FindOneAndDeleteOptions<T> : BaseDeleteOneOptions<T, CollectionSortBuilder<T>> where T : class
{
    /// <summary>
    /// Define the projection to apply on the returned document.
    /// </summary>
    public IProjectionBuilder Projection { get; set; }

    internal new object ToPayload(Filter<T> filter)
    {
        return new
        {
            filter = filter?.Serialize(),
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value),
            projection = Projection?.Projections?.ToDictionary(x => x.FieldName, x => x.Value) ?? new()
        };
    }
}
