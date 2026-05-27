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

using System;
using System.Collections.Generic;
using System.Linq;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Base class for find-and-rerank operation options.
/// </summary>
public abstract class BaseFindAndRerankOptions<T, TSort> : BasePaginatedFindOptions<T, BaseFindAndRerankOptions<T, TSort>>
    where T : class
    where TSort : FindAndRerankSortBuilder<T>
{
    /// <summary>
    /// Sort order for the query.
    /// </summary>
    public TSort Sort { get; set; }

    /// <summary>
    /// Whether to include similarity scores in the results.
    /// </summary>
    public bool? IncludeScores { get; set; }

    /// <summary>
    /// The field to use for the reranking step of a FindAndRerank operation.
    /// </summary>
    public string RerankOn { get; set; }

    /// <summary>
    /// The query string to rerank against in the reranking step of a FindAndRerank operation.
    /// </summary>
    public string RerankQuery { get; set; }

    /// <summary>
    /// A maximum number of results to retrieve for each of the vector and lexical sub-searches in a FindAndRerank operation.
    /// When specifying this parameter, <see cref="VectorLimit"/> and <see cref="LexicalLimit"/> cannot be set.
    /// </summary>
    public int? HybridLimits { get; set; }

    /// <summary>
    /// A maximum number of results to retrieve for the vector sub-search in a FindAndRerank operation.
    /// When specifying this parameter, <see cref="LexicalLimit"/> must be set as well and <see cref="HybridLimits"/> cannot be set.
    /// </summary>
    public int? VectorLimit { get; set; }

    /// <summary>
    /// A maximum number of results to retrieve for the lexical sub-search in a FindAndRerank operation.
    /// When specifying this parameter, <see cref="VectorLimit"/> must be set as well and <see cref="HybridLimits"/> cannot be set.
    /// </summary>
    public int? LexicalLimit { get; set; }

    internal override object ToPayload(Filter<T> filter, string pageState = null)
    {
        // enforce the constraints between HybridLimits, VectorLimit/LexicalLimit
        object hybridLimits = null;
        if (HybridLimits.HasValue) {
            if (VectorLimit.HasValue || LexicalLimit.HasValue) {
                throw new ArgumentException("Cannot set 'VectorLimit' or 'LexicalLimit' when using the shorthand 'HybridLimits'.");
            } else {
                hybridLimits = HybridLimits;
            }
        } else {
            // HybridLimits is null:
            if (VectorLimit.HasValue && LexicalLimit.HasValue) {
                hybridLimits = new Dictionary<string, object> {
                    ["$vector"] = VectorLimit,
                    ["$lexical"] = LexicalLimit
                };
            } else {
                if (VectorLimit.HasValue || LexicalLimit.HasValue) {
                    throw new ArgumentException("Cannot set only one of 'VectorLimit' and 'LexicalLimit'.");
                }
            }
        }

        Dictionary<string, object> sort = null;
        if (Sort != null && Sort._Sort != null && Sort._Sort.Name != null) {
            sort = new Dictionary<string, object> {
                [Sort._Sort.Name] = Sort._Sort.Value,
            };
        }

        var options = new Dictionary<string, object>();
        if (IncludeScores.HasValue)
            options["includeScores"] = IncludeScores.Value;
        if (IncludeSortVector.HasValue)
            options["includeSortVector"] = IncludeSortVector.Value;
        if (!string.IsNullOrEmpty(pageState ?? InitialPageState))
            options["pageState"] = pageState ?? InitialPageState;
        if (Limit.HasValue)
            options["limit"] = Limit.Value;
        if (!string.IsNullOrEmpty(RerankOn))
            options["rerankOn"] = RerankOn;
        if (!string.IsNullOrEmpty(RerankQuery))
            options["rerankQuery"] = RerankQuery;
        if (hybridLimits != null)
            options["hybridLimits"] = hybridLimits;

        return new
        {
            filter = filter?.Serialize(),
            sort = sort,
            projection = Projection?.Projections?.ToDictionary(x => x.FieldName, x => x.Value) ?? new(),
            options = options,
        };
    }

    internal override BaseFindAndRerankOptions<T, TSort> ShallowClone()
    {
        return (BaseFindAndRerankOptions<T, TSort>)MemberwiseClone();
    }
}

/// <summary>
/// Options for find-and-rerank queries on a collection.
/// </summary>
public sealed class CollectionFindAndRerankOptions<T> : BaseFindAndRerankOptions<T, CollectionFindAndRerankSortBuilder<T>> where T : class
{
}
