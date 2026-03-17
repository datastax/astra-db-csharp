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

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// Options to use when creating a new database.
/// </summary>
public class DatabaseCreationOptions
{
    /// <summary>
    /// Name of the database to be created.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }

    /// <summary>
    /// Which cloud provider should host the database?
    /// </summary>
    [JsonPropertyName("cloudProvider")]
    public CloudProviderType? CloudProvider { get; set; } = null;

    /// <summary>
    /// Database region.
    /// </summary>
    [JsonPropertyName("region")]
    public string Region { get; set; }

    /// <summary>
    /// Name of the initial keyspace (defaults to "default_keyspace")
    /// </summary>
    [JsonPropertyName("keyspace")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string Keyspace { get; set; } = null;

    /// <summary>
    /// Capacity units to use, defaults to 1.
    /// </summary>
    [JsonPropertyName("capacityUnits")]
    [JsonInclude]
    internal int CapacityUnits { get; set; } = 1;

    /// <summary>
    /// Tier to use, defaults to "serverless".
    /// </summary>
    [JsonPropertyName("tier")]
    [JsonInclude]
    internal string Tier { get; set; } = "serverless";

    /// <summary>
    /// Type of database, defaults to "vector".
    /// </summary>
    [JsonPropertyName("dbType")]
    [JsonInclude]
    internal string DatabaseType { get; set; } = "vector";
}