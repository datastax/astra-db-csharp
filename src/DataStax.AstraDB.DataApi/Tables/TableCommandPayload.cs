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


internal class TableCommandPayload
{
    /*
     * "createTable": {
            "name": "TABLE_NAME",
            "definition": {
                "columns": {
                    "COLUMN_NAME": "DATA_TYPE",
                    "COLUMN_NAME": "DATA_TYPE"
                },
                "primaryKey": "PRIMARY_KEY_DEFINITION"
            }
        }
     */
    [JsonPropertyName("name")]
    [JsonInclude]
    internal string Name { get; set; }

    [JsonPropertyName("definition")]
    [JsonInclude]
    internal TableDefinition Definition { get; set; }


}
