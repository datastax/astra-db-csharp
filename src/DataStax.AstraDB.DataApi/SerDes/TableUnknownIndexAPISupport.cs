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

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// API capability details returned for a table index type that does not map to a known SDK definition.
/// </summary>
public class TableUnknownIndexAPISupport
{
    /// <summary>
    /// Gets or sets a value indicating whether the Data API reports support for index creation.
    /// </summary>
    [JsonPropertyName("createIndex")]
    public bool CreateIndex { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the Data API reports support for filtering with this index.
    /// </summary>
    [JsonPropertyName("filter")]
    public bool Filter { get; set; }

    /// <summary>
    /// Gets or sets the CQL definition returned by the Data API for this index.
    /// </summary>
    [JsonPropertyName("cqlDefinition")]
    public string CQLDefinition { get; set; }
}
