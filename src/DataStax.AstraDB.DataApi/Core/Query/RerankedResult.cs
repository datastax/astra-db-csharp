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

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// A result document returned from a reranked (hybrid search) query, including its reranking scores.
/// </summary>
/// <typeparam name="T">The type of the result document.</typeparam>
public class RerankedResult<T>
{
    /// <summary>The result document.</summary>
    [JsonIgnore]
    public T Document { get; set; }

    /// <summary>The reranking scores associated with this result, keyed by score name.</summary>
    [JsonPropertyName("scores")]
    public Dictionary<string, object> Scores { get; set; }
}
