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
public class TableIndexMetadata
{
  [JsonPropertyName("name")]
  public string Name { get; set; }

  [JsonPropertyName("definition")]
  public TableIndexDefinition Definition { get; set; }

  [JsonPropertyName("indexType")]
  public string IndexType { get; set; }
}

public class TableIndexDefinition
{
  [JsonPropertyName("column")]
  public string Column { get; set; }

  [JsonPropertyName("options")]
  public Dictionary<string, bool> Options { get; set; }

}

public class TableIndexMetadataResult
{
  [JsonPropertyName("indexes")]
  public List<TableIndexMetadata> Indexes { get; set; }
}

/*
{
  "status": {
    "indexes": [
      {
        "name": "summary_genres_vector_index",
        "definition": {
          "column": "summary_genres_vector",
          "options": { "metric": "cosine", "sourceModel": "other" }
        },
        "indexType": "vector"
      },
      {
        "name": "rating_index",
        "definition": { "column": "rating", "options": {} },
        "indexType": "regular"
      }
    ]
  }
}
*/

public class TableIndexOptions
{
  [JsonPropertyName("name")]
  public string Name { get; set; }

  [JsonPropertyName("column")]
  public string Column { get; set; }

  [JsonPropertyName("options")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public IndexOptionFlags Options { get; set; }
}

public class IndexOptionFlags
{
  [JsonPropertyName("ascii")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public bool? Ascii { get; set; }

  [JsonPropertyName("normalize")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public bool? Normalize { get; set; }

  [JsonPropertyName("caseSensitive")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public bool? CaseSensitive { get; set; }
}

/*
"createIndex": {
    "name": "INDEX_NAME",
    "definition": {
      "column": "COLUMN_NAME",
      "options": {
        "ascii": BOOLEAN,
        "normalize": BOOLEAN,
        "caseSensitive": BOOLEAN
      }
    }
*/