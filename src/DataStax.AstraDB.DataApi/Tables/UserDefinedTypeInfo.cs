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
/// Description of an existing User Defined Type
/// </summary>
public class UserDefinedTypeInfo
{
    /// <summary>
    /// The type
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; set; }

    /// <summary>
    /// Name
    /// </summary>
    [JsonPropertyName("udtName")]
    public string Name { get; set; }

    /// <summary>
    /// List of fields for this User Defined Type
    /// </summary>
    [JsonPropertyName("definition")]
    public TypeDefinitionInfo Definition { get; set; }

    /// <summary>
    /// Information regarding API support for this Type
    /// </summary>
    [JsonPropertyName("apiSupport")]
    public TypeApiSupportInfo ApiSupport { get; set; }
}

/*
{
        "type": "userDefined",
        "udtName": "address",
        "definition": {
          "fields": {
            "city": {
              "type": "text"
            },
            "country": {
              "type": "text"
            }
          }
        },
        "apiSupport": {
          "createTable": true,
          "insert": true,
          "read": true,
          "filter": false,
          "cqlDefinition": "demo.address"
        }
*/

/// <summary>
/// Information regarding API support for a User Defined Type
/// </summary>
public class TypeApiSupportInfo
{
    /// <summary>
    /// Can a table be created using this Type?
    /// </summary>
    [JsonPropertyName("createTable")]
    public bool CanCreateTable { get; set; }

    /// <summary>
    /// Can data be inserted using this Type?
    /// </summary>
    [JsonPropertyName("insert")]
    public bool CanInsert { get; set; }

    /// <summary>
    /// Can this type be read from the DB?
    /// </summary>
    [JsonPropertyName("read")]
    public bool CanRead { get; set; }

    /// <summary>
    /// Can this type be used for filtering?
    /// </summary>
    [JsonPropertyName("filter")]
    public bool CanFilter { get; set; }

    /// <summary>
    /// CqlDefinition for this Type
    /// </summary>
    [JsonPropertyName("cqlDefinition")]
    public string CqlDefinition { get; set; }
}

/// <summary>
/// List of Fields for a User Defined Type
/// </summary>
public class TypeDefinitionInfo
{
    /// <summary>
    /// Field types, by column name
    /// </summary>
    [JsonPropertyName("fields")]
    public Dictionary<string, FieldTypeInfo> Fields { get; set; }
}

/// <summary>
/// DataApi Type for a specific field
/// </summary>
public class FieldTypeInfo
{
    /// <summary>
    /// The type :)
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; set; }
}