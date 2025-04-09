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
/// Vector options for a collection definition
/// </summary>
public class VectorOptions
{
    /// <summary>
    /// The dimension of the vector
    /// </summary>
    [JsonPropertyName("dimension")]
    public int? Dimension { get; set; }

    /// <summary>
    /// The similarity metric to use
    /// </summary>
    [JsonPropertyName("metric")]
    public SimilarityMetric Metric { get; set; }

    /// <summary>
    /// Options for the service providing the vectorization
    /// </summary>
    [JsonPropertyName("service")]
    public VectorServiceOptions Service { get; set; }
}
