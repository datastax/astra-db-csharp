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

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Admin;

public class DatabaseInfo
{
    [JsonPropertyName("availableActions")]
    public List<string> AvailableActions { get; set; }

    [JsonPropertyName("cost")]
    public DatabaseCost Cost { get; set; }

    [JsonPropertyName("cqlshUrl")]
    public string CqlshUrl { get; set; }

    [JsonPropertyName("creationTime")]
    public DateTime CreationTime { get; set; }

    [JsonPropertyName("dataEndpointUrl")]
    public string DataEndpointUrl { get; set; }

    [JsonPropertyName("grafanaUrl")]
    public string GrafanaUrl { get; set; }

    [JsonPropertyName("graphqlUrl")]
    public string GraphqlUrl { get; set; }

    [JsonPropertyName("id")]
    public string Id { get; set; }

    [JsonPropertyName("info")]
    public DatabaseDetailsInfo Info { get; set; }

    [JsonPropertyName("lastUsageTime")]
    public DateTime LastUsageTime { get; set; }

    [JsonPropertyName("metrics")]
    public DatabaseMetrics Metrics { get; set; }

    [JsonPropertyName("observedStatus")]
    public string ObservedStatus { get; set; }

    [JsonPropertyName("orgId")]
    public string OrgId { get; set; }

    [JsonPropertyName("ownerId")]
    public string OwnerId { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; }

    [JsonPropertyName("storage")]
    public DatabaseStorage Storage { get; set; }

    [JsonPropertyName("terminationTime")]
    public DateTime TerminationTime { get; set; }
}

public class DatabaseCost
{
    [JsonPropertyName("costPerDayCents")]
    public decimal CostPerDayCents { get; set; }

    [JsonPropertyName("costPerDayMRCents")]
    public decimal CostPerDayMRCents { get; set; }

    [JsonPropertyName("costPerDayParkedCents")]
    public decimal CostPerDayParkedCents { get; set; }

    [JsonPropertyName("costPerHourCents")]
    public decimal CostPerHourCents { get; set; }

    [JsonPropertyName("costPerHourMRCents")]
    public decimal CostPerHourMRCents { get; set; }

    [JsonPropertyName("costPerHourParkedCents")]
    public decimal CostPerHourParkedCents { get; set; }

    [JsonPropertyName("costPerMinCents")]
    public decimal CostPerMinCents { get; set; }

    [JsonPropertyName("costPerMinMRCents")]
    public decimal CostPerMinMRCents { get; set; }

    [JsonPropertyName("costPerMinParkedCents")]
    public decimal CostPerMinParkedCents { get; set; }

    [JsonPropertyName("costPerMonthCents")]
    public decimal CostPerMonthCents { get; set; }

    [JsonPropertyName("costPerMonthMRCents")]
    public decimal CostPerMonthMRCents { get; set; }

    [JsonPropertyName("costPerMonthParkedCents")]
    public decimal CostPerMonthParkedCents { get; set; }

    [JsonPropertyName("costPerNetworkGbCents")]
    public decimal CostPerNetworkGbCents { get; set; }

    [JsonPropertyName("costPerReadGbCents")]
    public decimal CostPerReadGbCents { get; set; }

    [JsonPropertyName("costPerWrittenGbCents")]
    public decimal CostPerWrittenGbCents { get; set; }
}

public class DatabaseDetailsInfo
{
    [JsonPropertyName("capacityUnits")]
    public int CapacityUnits { get; set; }

    [JsonPropertyName("cloudProvider")]
    public string CloudProvider { get; set; }

    [JsonPropertyName("datacenters")]
    public List<DatacenterInfo> Datacenters { get; set; }

    [JsonPropertyName("dbType")]
    public string DbType { get; set; }

    [JsonPropertyName("keyspace")]
    public string Keyspace { get; set; }

    [JsonPropertyName("keyspaces")]
    public List<string> Keyspaces { get; set; }

    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("region")]
    public string Region { get; set; }

    [JsonPropertyName("tier")]
    public string Tier { get; set; }
}

public class DatacenterInfo
{
    [JsonPropertyName("capacityUnits")]
    public int CapacityUnits { get; set; }

    [JsonPropertyName("cloudProvider")]
    public string CloudProvider { get; set; }

    [JsonPropertyName("dateCreated")]
    public DateTime DateCreated { get; set; }

    [JsonPropertyName("id")]
    public string Id { get; set; }

    [JsonPropertyName("isPrimary")]
    public bool IsPrimary { get; set; }

    [JsonPropertyName("name")]
    public string Name { get; set; }
}

public class DatabaseMetrics
{
    [JsonPropertyName("errorsTotalCount")]
    public long ErrorsTotalCount { get; set; }

    [JsonPropertyName("liveDataSizeBytes")]
    public long LiveDataSizeBytes { get; set; }

    [JsonPropertyName("readRequestsTotalCount")]
    public long ReadRequestsTotalCount { get; set; }

    [JsonPropertyName("writeRequestsTotalCount")]
    public long WriteRequestsTotalCount { get; set; }
}

public class DatabaseStorage
{
    [JsonPropertyName("displayStorage")]
    public int DisplayStorage { get; set; }

    [JsonPropertyName("nodeCount")]
    public int NodeCount { get; set; }

    [JsonPropertyName("replicationFactor")]
    public int ReplicationFactor { get; set; }

    [JsonPropertyName("totalStorage")]
    public int TotalStorage { get; set; }
}