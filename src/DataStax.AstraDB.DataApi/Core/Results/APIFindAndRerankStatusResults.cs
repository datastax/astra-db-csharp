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

namespace DataStax.AstraDB.DataApi.Core.Results;

internal class DocumentResponse
{
    [JsonInclude]
    [JsonPropertyName("scores")]
    internal Dictionary<string, float?> Scores { get; set; }
}

/// <summary>
/// The 'status' portion of a find-and-rerank query response.
/// </summary>
internal class APIFindAndRerankStatusResults
{
    /// <summary>
    /// The side information returned by the query along with the documents
    /// (e.g. scores for a findAndRerank command).
    /// </summary>
    [JsonInclude]
    [JsonPropertyName("documentResponses")]
    internal List<DocumentResponse> DocumentResponses { get; set; }

    /// <summary>
    /// The vector used in vector search, when requested in the command invocation.
    /// </summary>
    [JsonInclude]
    [JsonPropertyName("sortVector")]
    internal float[] SortVector { get; set; }
}
