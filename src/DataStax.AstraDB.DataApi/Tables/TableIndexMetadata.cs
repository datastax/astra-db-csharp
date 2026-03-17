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

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Metadata about an existing table index, as returned by the Data API.
/// </summary>
public class TableIndexMetadata
{
    /// <summary>
    /// The name of the index.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }

    /// <summary>
    /// The definition of the index, including the target column and options.
    /// </summary>
    [JsonPropertyName("definition")]
    public TableIndexDefinition Definition { get; set; }

    /// <summary>
    /// The type of the index (e.g., "regular" or "vector").
    /// </summary>
    [JsonPropertyName("indexType")]
    public string IndexType { get; set; }
}

//{"indexes":[{"name":"author_index",
// "definition":{"column":"Author","options":{"metric":"cosine","sourceModel":"other"}},"indexType":"vector"},{"name":"number_of_pages_index","definition":{"column":"NumberOfPages","options":{}},"indexType":"regular"}]}}