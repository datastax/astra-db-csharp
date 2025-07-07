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
/// Configuration for the reranking service
/// </summary>
public class RerankServiceOptions
{
    /// <summary>
    /// The name of the model to use for reranking
    /// </summary>
    [JsonPropertyName("modelName")]
    public string ModelName { get; set; }

    /// <summary>
    /// The provider of the reranking service
    /// </summary>
    [JsonPropertyName("provider")]
    public string Provider { get; set; }
}
