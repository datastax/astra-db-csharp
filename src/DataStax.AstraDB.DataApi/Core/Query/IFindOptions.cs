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

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Represents options for find operations.
/// </summary>
/// <typeparam name="T">The type of the document/entity.</typeparam>
/// <typeparam name="TSort">The type of the sort builder.</typeparam>
internal interface IFindOptions<T, TSort> where TSort : SortBuilder<T>
{
    internal Filter<T> Filter { get; set; }

    internal string PageState { get; set; }

    internal TSort Sort { get; set; }

    internal IProjectionBuilder Projection { get; set; }

    internal bool? IncludeSimilarity { get; set; }
}
