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
/// Metadata for a table index definition returned by the Data API when the SDK does not recognize the index type.
/// </summary>
public class TableUnknownIndexDefinition : TableBaseIndexDefinition
{
    /// <summary>
    /// Gets or sets the API support metadata returned for an index type that does not map to a known concrete definition.
    /// </summary>
    [JsonPropertyName("apiSupport")]
    public TableUnknownIndexAPISupport APISupport { get; set; }

    internal override string IndexCreationCommandName => null;
}
