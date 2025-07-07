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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Options for configuring document reranking in the collection
/// </summary>
public class RerankOptions
{
    /// <summary>
    /// Whether reranking is enabled
    /// </summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Configuration for the reranking service
    /// </summary>
    [JsonPropertyName("service")]
    public RerankServiceOptions Service { get; set; }

    //TODO: When implementing fluent option, have default for currently available service:
    // public RerankOptions() { Service = new RerankServiceOptions() { ModelName = "nvidia/llama-3.2-nv-rerankqa-1b-v2", Provider = "nvidia" }; }
}
