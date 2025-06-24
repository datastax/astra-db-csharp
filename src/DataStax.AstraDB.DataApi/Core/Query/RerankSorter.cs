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

using DataStax.AstraDB.DataApi.Core.Commands;
using System;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class RerankSorter<T, TResult> where T : class where TResult : class
{

    private readonly Func<Command> _commandFactory;
    private readonly CommandOptions _commandOptions;
    private readonly FindAndRerankOptions<T> _findOptions;

    internal RerankSorter(Func<Command> commandFactory, Filter<T> filter, CommandOptions commandOptions)
    {
        _commandFactory = commandFactory;
        _commandOptions = commandOptions;
        _findOptions = new FindAndRerankOptions<T>() { Filter = filter };
    }

    /// <summary>
    /// Adds a hybrid sort using a combined string to use for lexical and vectorize parameters.
    /// </summary>
    /// <param name="searchString">Combined string to use for lexical and vectorize parameters.</param>
    /// <returns>The document sort builder.</returns>
    public RerankEnumerator<T, TResult> Sort(string combinedSearchString)
    {
        _findOptions.Sorts.Add(Query.Sort.Hybrid(combinedSearchString));
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Adds a hybrid sort using separate parameters, one for the lexical search and the second string to vectorize for the vector ordering.
    /// </summary>
    /// <param name="lexical">The lexical search string.</param>
    /// <param name="vectorize">The string to vectorize for the vector ordering.</param>
    /// <returns>The document sort builder.</returns>
    public RerankEnumerator<T, TResult> Sort(string lexical, string vectorize)
    {
        _findOptions.Sorts.Add(Query.Sort.Hybrid(lexical, vectorize));
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }

    /// <summary>
    /// Adds a hybrid sort using separate parameters, one for the lexical search and the second float array for the vector ordering.
    /// </summary>
    /// <param name="lexical">The lexical search string.</param>
    /// <param name="vector">The vector parameter.</param>
    /// <returns>The document sort builder.</returns>
    public RerankEnumerator<T, TResult> Sort(string lexical, float[] vector)
    {
        _findOptions.Sorts.Add(Query.Sort.Hybrid(lexical, vector));
        _findOptions.RerankOn = DataApiKeywords.Lexical;
        _findOptions.RerankQuery = lexical;
        return new RerankEnumerator<T, TResult>(_commandFactory, _findOptions, _commandOptions);
    }
}