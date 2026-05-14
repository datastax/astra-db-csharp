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
/// Base class for replace-one operation options.
/// </summary>
public abstract class BaseReplaceOneOptions<T, TSort> : CommandOptions
    where T : class
    where TSort : SortBuilder<T>
{
    /// <summary>
    /// Sort order for determining which document to replace when multiple match the filter.
    /// </summary>
    public TSort Sort { get; set; }

    /// <summary>
    /// Whether to insert the document if no matching document is found or not.
    /// </summary>
    public bool Upsert { get; set; }

    internal object ToPayload(Filter<T> filter, T replacement)
    {
        return new
        {
            filter = filter?.Serialize() ?? new(),
            replacement,
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value),
            options = new
            {
                upsert = Upsert ? true : (bool?)null
            }
        };
    }
}

/// <summary>
/// Options for replacing a document in a collection.
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class CollectionReplaceOneOptions<T> : BaseReplaceOneOptions<T, CollectionSortBuilder<T>> where T : class
{
}

/// <summary>
/// Options for finding and replacing a single document in a collection.
/// </summary>
/// <typeparam name="T"></typeparam>
public class CollectionFindOneAndReplaceOptions<T> : BaseReplaceOneOptions<T, CollectionSortBuilder<T>> where T : class
{
    /// <summary>
    /// Define the projection to apply on the returned document.
    /// </summary>
    public IProjectionBuilder Projection { get; set; }

    /// <summary>
    /// Whether to return the document before or after the replacement.
    /// </summary>
    public ReturnDocumentDirective? ReturnDocument { get; set; }

    internal new object ToPayload(Filter<T> filter, T replacement)
    {
        return new
        {
            filter = filter?.Serialize() ?? new(),
            replacement,
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
