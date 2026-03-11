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

using DataStax.AstraDB.DataApi.Core;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Admin;

public class DatabaseCreationOptions
{
    public DatabaseCreationOptions(string name, CloudProviderType cloudProvider, string region)
    {
        Name = name;
        CloudProvider = cloudProvider;
        Region = region;
    }

    public DatabaseCreationOptions(string name, CloudProviderType cloudProvider, string region, string keyspace)
    {
        Name = name;
        CloudProvider = cloudProvider;
        Region = region;
        Keyspace = keyspace;
    }

    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("cloudProvider")]
    public CloudProviderType CloudProvider { get; set; }

    [JsonPropertyName("region")]
    public string Region { get; set; }

    [JsonPropertyName("keyspace")]
    public string Keyspace { get; set; } = Database.DefaultKeyspace;

    [JsonPropertyName("capacityUnits")]
    [JsonInclude]
    internal int CapacityUnits { get; set; } = 1;

    [JsonPropertyName("tier")]
    [JsonInclude]
    internal string Tier { get; set; } = "serverless";

    [JsonPropertyName("dbType")]
    [JsonInclude]
    internal string DatabaseType { get; set; } = "vector";
}