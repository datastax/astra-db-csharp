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
/// Options for a collection's behavior
/// </summary>
public class CollectionDefinition
{
    /// <summary>
    /// Settings for generating ids
    /// </summary>
    [JsonPropertyName("defaultId")]
    public DefaultIdOptions DefaultId { get; set; }

    /// <summary>
    /// Vector specifications for the collection
    /// </summary>
    [JsonPropertyName("vector")]
    public VectorOptions Vector { get; set; }

    /// <summary>
    /// Overrides for document indexing
    /// </summary>
    [JsonPropertyName("indexing")]
    public IndexingOptions Indexing { get; set; }
}




