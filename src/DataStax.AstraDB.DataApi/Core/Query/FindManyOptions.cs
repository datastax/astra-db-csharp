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
/// Base class for find-many operation options.
/// </summary>
public abstract class BaseFindManyOptions<T, TSort> : CommandOptions 
    where T : class
    where TSort : SortBuilder<T>
{
    /// <summary>
    /// Creates a shallow copy of this options object.
    /// </summary>
    public BaseFindManyOptions<T, TSort> Clone()
    {
        return (BaseFindManyOptions<T, TSort>)MemberwiseClone();
    }
    
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
    
    /// <summary>
    /// Whether to include the sort vector in the results.
    /// </summary>
    public bool? IncludeSortVector { get; set; }
    
    /// <summary>
    /// The initial page state used to resume pagination from a previous find-many operation.
    /// </summary>
    public string InitialPageState { get; set; }
    
    /// <summary>
    /// The number of documents to skip before starting to return documents.
    /// </summary>
    public int? Skip { get; set; }
    
    /// <summary>
    /// The maximum number of documents to return.
    /// </summary>
    public int? Limit { get; set; }

    internal object ToPayload(Filter<T> filter, string pageState = null)
    {
        return new
        {
            filter = filter?.Serialize(),
            sort = Sort?.Sorts?.ToDictionary(x => x.Name, x => x.Value),
            projection = Projection?.Projections?.ToDictionary(x => x.FieldName, x => x.Value),
            options = new
            {
                includeSimilarity = IncludeSimilarity,
                includeSortVector = IncludeSortVector,
                pageState = pageState ?? InitialPageState,
                skip = Skip,
                limit = Limit,
            },
        };
    }
}

/// <summary>
/// Options for finding multiple documents in a collection.
/// </summary>
public sealed class CollectionFindManyOptions<T> : BaseFindManyOptions<T, CollectionSortBuilder<T>> where T : class
{
}

/// <summary>
/// Options for finding multiple rows in a table.
/// </summary>
public sealed class TableFindManyOptions<T> : BaseFindManyOptions<T, TableSortBuilder<T>> where T : class
{
}
