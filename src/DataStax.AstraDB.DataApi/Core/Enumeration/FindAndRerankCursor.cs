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
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Enumeration;

/// <summary>
/// A fluent API cursor for running a find-and-rerank query and enumerating records or rows with filtering, sorting, and projection capabilities.
/// 
/// This cursor extends <see cref="PaginatedCursor{T,TResult,TOptions,TCursor}"/> to provide query-specific operations like sorting or setting rerank details.
/// 
/// It supports both synchronous and asynchronous iteration patterns.
/// 
/// Use the fluent methods to refine your query, then iterate using foreach, LINQ, or manual cursor navigation.
/// </summary>
/// <typeparam name="T">The type representing the record or row being queried.</typeparam>
/// <typeparam name="TResult">The type to deserialize the results to (e.g., when using projections).</typeparam>
/// <typeparam name="TSort">The type of sort builder to use (such as <see cref="CollectionFindAndRerankSortBuilder{T}"/>).</typeparam>
/// <typeparam name="TCursor">The concrete cursor type for fluent method chaining.</typeparam>
public abstract class FindAndRerankCursor<T, TResult, TSort, TCursor> : PaginatedCursor<T, TResult, BaseFindAndRerankOptions<T, TSort>, TCursor>
    where T : class
    where TResult : class
    where TSort : FindAndRerankSortBuilder<T>
    where TCursor : FindAndRerankCursor<T, TResult, TSort, TCursor>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FindAndRerankCursor{T, TResult, TSort, TCursor}"/> class.
    /// </summary>
    /// <param name="filter">The filter to apply.</param>
    /// <param name="options">The find options to use.</param>
    /// <param name="fetchPageFunc">The function to fetch pages of results.</param>
    internal FindAndRerankCursor(Filter<T> filter, BaseFindAndRerankOptions<T, TSort> options,
        FetchPageFunc<TResult, TCursor> fetchPageFunc) : base(filter, options, fetchPageFunc)
    {
    }

    /// <summary>
    /// Specifies a sort to apply to the query.
    /// </summary>
    /// <param name="sort">The sort to apply.</param>
    /// <returns>A new cursor instance with the updated sort.</returns>
    /// <example>
    /// <code>
    /// var sort = Builders&lt;MyRecord&gt;.CollectionFindAndRerankSort.Hybrid(d => d.Description);
    /// var cursor = collection.FindAndRerank().Sort(sort);
    /// </code>
    /// </example>
    public TCursor Sort(TSort sort)
    {
        return UpdateOptions(options => options.Sort = sort);
    }

    /// <summary>
    /// Specifies the field to rerank documents against.
    /// </summary>
    /// <param name="expression">The expression to use to get the field name.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <example>
    /// <code>
    /// var cursor = collection.FindAndRerank().RerankOn(d => d.Title);
    /// </code>
    /// </example>
    public TCursor RerankOn<TField>(Expression<Func<T, TField>> expression)
    {
        return UpdateOptions(options => options.RerankOn = expression.GetMemberNameTree());
    }

    /// <summary>
    /// Specifies the field to rerank documents against.
    /// </summary>
    /// <param name="rerankOn">The name of the field to use for reranking.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <example>
    /// <code>
    /// var cursor = collection.FindAndRerank().RerankOn("title");
    /// </code>
    /// </example>
    /// <remarks>
    /// We recommend using the strongly-typed version <see cref="RerankOn{TField}(Expression{Func{T, TField}})"/>.
    /// </remarks>
    public TCursor RerankOn(string rerankOn)
    {
        return UpdateOptions(options => options.RerankOn = rerankOn);
    }

    /// <summary>
    /// Specifies the query string to rerank against in the reranking step.
    /// </summary>
    /// <param name="rerankQuery">The query string to use for reranking.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <example>
    /// <code>
    /// var cursor = collection.FindAndRerank().RerankQuery("a tree on a grassy hillside");
    /// </code>
    /// </example>
    public TCursor RerankQuery(string rerankQuery)
    {
        return UpdateOptions(options => options.RerankQuery = rerankQuery);
    }

    /// <summary>
    /// Specifies a maximum number of documents to fetch for both the vector and the lexical sub-searches.
    /// </summary>
    /// <param name="hybridLimits">The maximum count of returned documents per each sub-search contributing to the rerank step.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <example>
    /// <code>
    /// var cursor = collection.FindAndRerank().HybridLimits(12);
    /// </code>
    /// </example>
    public TCursor HybridLimits(int hybridLimits)
    {
        return UpdateOptions(options => {
            options.HybridLimits = hybridLimits;
            options.VectorLimit = null;
            options.LexicalLimit = null;
        });
    }

    /// <summary>
    /// Specifies a maximum number of documents to fetch for both the vector and the lexical sub-searches.
    /// </summary>
    /// <param name="vectorLimit">The maximum count of returned documents for the vector sub-search.</param>
    /// <param name="lexicalLimit">The maximum count of returned documents for the lexical sub-search.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <example>
    /// <code>
    /// var cursor = collection.FindAndRerank().HybridLimits(vectorLimit: 25, lexicalLimit: 15);
    /// </code>
    /// </example>
    public TCursor HybridLimits(int vectorLimit, int lexicalLimit)
    {
        return UpdateOptions(options => {
            options.HybridLimits = null;
            options.VectorLimit = vectorLimit;
            options.LexicalLimit = lexicalLimit;
        });
    }

    /// <summary>
    /// Specifies whether to return the scores with the results.
    /// </summary>
    /// <param name="include">Whether to include the scores. Defaults to true.</param>
    /// <returns>A new cursor instance with the updated setting.</returns>
    /// <remarks>
    /// TODO a note on the Scores being always there, just populated/empty depending on this setting.
    /// </remarks>
    /// <example>
    /// <code>
    /// TODO example
    /// </code>
    /// </example>
    public TCursor IncludeScores(bool include = true)
    {
        return UpdateOptions(options => options.IncludeScores = include);
    }

    internal override TCursor UpdateOptions(Action<BaseFindAndRerankOptions<T, TSort>> optionsUpdater)
    {
        if (State != CursorState.Idle)
        {
            throw new CursorException("Cursors must be idle when building their options", State);
        }
        var newOptions = FindOptions.ShallowClone();
        optionsUpdater(newOptions);
        return CloneWith(CurrentFilter, newOptions);
    }
}
