using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core;
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

public class VectorServiceOptions
{
    /// <summary>
    /// The provider for vectorization
    /// </summary>
    [JsonPropertyName("provider")]
    public string Provider { get; set; }

    /// <summary>
    /// The name of the model to use
    /// </summary>
    [JsonPropertyName("modelName")]
    public string ModelName { get; set; }

    /// <summary>
    /// Authentication details for the vectorization provider
    /// </summary>
    [JsonPropertyName("authentication")]
    public Dictionary<string, object> Authentication { get; set; }

    /// <summary>
    /// Additional parameters for the vectorization provider
    /// </summary>
    [JsonPropertyName("parameters")]
    public Dictionary<string, object> Parameters { get; set; }
}
