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

using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

/// <summary>
/// Internal class representing options for the Find API.
/// </summary>
internal class FindApiOptions
{
    [JsonInclude]
    [JsonPropertyName("skip")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal int? Skip { get; set; }

    [JsonInclude]
    [JsonPropertyName("limit")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal int? Limit { get; set; }

    [JsonInclude]
    [JsonPropertyName("includeSimilarity")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal bool? IncludeSimilarity { get; set; }

    [JsonInclude]
    [JsonPropertyName("includeSortVector")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal bool? IncludeSortVector { get; set; }

    [JsonInclude]
    [JsonPropertyName("pageState")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal string PageState { get; set; }
}
