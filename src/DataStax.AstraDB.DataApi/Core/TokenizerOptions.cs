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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Tokenizer configuration
/// </summary>
public class TokenizerOptions
{
    /// <summary>
    /// Name of the tokenizer (e.g., "standard")
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }

    /// <summary>
    /// Additional tokenizer arguments
    /// </summary>
    [JsonPropertyName("args")]
    public Dictionary<string, object> Arguments { get; set; } = new();
}
