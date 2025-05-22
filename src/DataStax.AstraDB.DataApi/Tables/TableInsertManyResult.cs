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

using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.SerDes;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

[JsonConverter(typeof(TableInsertManyResultConverter))]
public class TableInsertManyResult
{
  /// <summary>
  /// The primary key schema of the returned rows
  /// </summary>
  [JsonPropertyName("primaryKeySchema")]
  public Dictionary<string, PrimaryKeySchema> PrimaryKeys { get; set; }

  /// <summary>
  /// A list of the Ids of the inserted documents
  /// </summary>
  [JsonPropertyName("insertedIds")]
  public List<List<object>> InsertedIds { get; set; } = new List<List<object>>();

  /// <summary>
  /// A list of the document responses
  /// </summary>
  [JsonPropertyName("documentResponses")]
  public List<DocumentInsertResult> DocumentResponses { get; set; } = new List<DocumentInsertResult>();

  /// <summary>
  /// The number of documents that were inserted
  /// </summary>
  [JsonIgnore]
  public int InsertedCount => InsertedIds.Count != 0 ? InsertedIds.Count : DocumentResponses.Count;
}

/*
{
  "status": {
    "primaryKeySchema": {
      "email": {
        "type": "ascii"
      },
      "graduation_year": {
        "type": "int"
      }
    },
    "insertedIds": [
      [
        "tal@example.com",
        2024
      ],
      [
        "sami@example.com",
        2024
      ],
      [
        "kiran@example.com",
        2024
      ]
    ]
  }
}

// document response
{
  "status": {
    "primaryKeySchema": {
      "email": {
        "type": "ascii"
      }
    },
    "documentResponses": [
      {"_id":["tal@example.com"], "status":"OK"},
      {"_id":["sami@example.com"], "status":"OK"},
      {"_id":["kirin@example.com"], "status":"OK"}
    ]
  }
}
*/