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
using System.Linq.Expressions;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A sort builder specific for a FindAndRerank operation on a collection.
/// </summary>
/// <typeparam name="T">The type of the document</typeparam>
public class CollectionFindAndRerankSortBuilder<T> : FindAndRerankSortBuilder<T>
{

    /// <summary>
    /// Adds a hybrid sort by specifying a string value to use as query string
    /// both for the vector and the lexical sub-searches. Requires vectorize on the collection.
    /// </summary>
    /// <param name="hybrid">The query string for the sub-searches.</param>
    /// <returns>The collection sort builder.</returns>
    public CollectionFindAndRerankSortBuilder<T> Hybrid(string hybrid)
    {
        _Sort = Sort.Hybrid(hybrid);
        return this;
    }

    /// <summary>
    /// Adds a hybrid sort by specifying a vector for the vector sub-search
    /// and a string for the lexical sub-search.
    /// </summary>
    /// <param name="vector">The query vector for the ANN sub-search. The dimension must match the collection's.</param>
    /// <param name="lexical">The query string for the lexical sub-search.</param>
    /// <returns>The collection sort builder.</returns>
    public CollectionFindAndRerankSortBuilder<T> Hybrid(float[] vector, string lexical)
    {
        _Sort = Sort.Hybrid(vector, lexical);
        return this;
    }

    /// <summary>
    /// Adds a hybrid sort by specifying two different query strings, one for the vector sub-search
    /// (through vectorize, which must be configured for the collection) and one for the lexical sub-search.
    /// </summary>
    /// <param name="vectorize">The query string for the ANN sub-search.</param>
    /// <param name="lexical">The query string for the lexical sub-search.</param>
    /// <returns>The collection sort builder.</returns>
    public CollectionFindAndRerankSortBuilder<T> Hybrid(string vectorize, string lexical)
    {
        _Sort = Sort.Hybrid(vectorize: vectorize, lexical: lexical);
        return this;
    }
}
