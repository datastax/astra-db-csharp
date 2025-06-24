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
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Configuration for the analyzer
/// </summary>
public class AnalyzerOptions
{
    /// <summary>
    /// Tokenizer configuration
    /// </summary>
    [JsonPropertyName("tokenizer")]
    public TokenizerOptions Tokenizer { get; set; } = new();

    /// <summary>
    /// List of filters to apply
    /// </summary>
    [JsonIgnore]
    public List<string> Filters { get; set; } = new();

    /// <summary>
    /// List of character filters to apply
    /// </summary>
    [JsonPropertyName("charFilters")]
    public List<string> CharacterFilters { get; set; } = new();

    [JsonPropertyName("filters")]
    [JsonInclude]
    internal List<FilterOptions> FilterOptions
    {
        get
        {
            return Filters.Select(f => new FilterOptions() { Name = f }).ToList();
        }
    }
}
