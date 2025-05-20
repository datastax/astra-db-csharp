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

public class TableIndexMetadata
{
  [JsonPropertyName("name")]
  public string Name { get; set; }

  [JsonPropertyName("definition")]
  public TableIndexDefinition Definition { get; set; }

  [JsonPropertyName("indexType")]
  public string IndexType { get; set; }
}

//{"indexes":[{"name":"author_index",
// "definition":{"column":"Author","options":{"metric":"cosine","sourceModel":"other"}},"indexType":"vector"},{"name":"number_of_pages_index","definition":{"column":"NumberOfPages","options":{}},"indexType":"regular"}]}}