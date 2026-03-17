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

/// <summary>
/// Contains the raw database metadata returned by the Astra DevOps API.
/// </summary>
public class RawDatabaseInfo
{
    /// <summary>
    /// The list of actions currently available for this database.
    /// </summary>
    [JsonPropertyName("availableActions")]
    public List<string> AvailableActions { get; set; }

    /// <summary>
    /// The cost information associated with this database.
    /// </summary>
    [JsonPropertyName("cost")]
    public RawDatabaseCost Cost { get; set; }

    /// <summary>
    /// The URL for accessing the database via CQL shell.
    /// </summary>
    [JsonPropertyName("cqlshUrl")]
    public string CqlshUrl { get; set; }

    /// <summary>
    /// The date and time when the database was created.
    /// </summary>
    [JsonPropertyName("creationTime")]
    public DateTime CreationTime { get; set; }

    /// <summary>
    /// The Data API endpoint URL for this database.
    /// </summary>
    [JsonPropertyName("dataEndpointUrl")]
    public string DataEndpointUrl { get; set; }

    /// <summary>
    /// The Grafana metrics dashboard URL for this database.
    /// </summary>
    [JsonPropertyName("grafanaUrl")]
    public string GrafanaUrl { get; set; }

    /// <summary>
    /// The GraphQL API URL for this database.
    /// </summary>
    [JsonPropertyName("graphqlUrl")]
    public string GraphqlUrl { get; set; }

    /// <summary>
    /// The unique identifier of the database.
    /// </summary>
    [JsonPropertyName("id")]
    public string Id { get; set; }

    /// <summary>
    /// The detailed configuration and metadata for this database.
    /// </summary>
    [JsonPropertyName("info")]
    public RawDatabaseDetailsInfo Info { get; set; }

    /// <summary>
    /// The date and time the database was last used.
    /// </summary>
    [JsonPropertyName("lastUsageTime")]
    public DateTime LastUsageTime { get; set; }

    /// <summary>
    /// The usage metrics for this database.
    /// </summary>
    [JsonPropertyName("metrics")]
    public RawDatabaseMetrics Metrics { get; set; }

    /// <summary>
    /// The observed (actual) status of the database.
    /// </summary>
    [JsonPropertyName("observedStatus")]
    public string ObservedStatus { get; set; }

    /// <summary>
    /// The identifier of the organization that owns this database.
    /// </summary>
    [JsonPropertyName("orgId")]
    public string OrgId { get; set; }

    /// <summary>
    /// The identifier of the user who owns this database.
    /// </summary>
    [JsonPropertyName("ownerId")]
    public string OwnerId { get; set; }

    /// <summary>
    /// The current status of the database (e.g., ACTIVE, PENDING).
    /// </summary>
    [JsonPropertyName("status")]
    public string Status { get; set; }

    /// <summary>
    /// The storage configuration for this database.
    /// </summary>
    [JsonPropertyName("storage")]
    public RawDatabaseStorage Storage { get; set; }

    /// <summary>
    /// The date and time when the database was or will be terminated.
    /// </summary>
    [JsonPropertyName("terminationTime")]
    public DateTime TerminationTime { get; set; }
}

/// <summary>
/// Contains cost information for an Astra DB database, expressed in cents.
/// </summary>
public class RawDatabaseCost
{
    /// <summary>
    /// The cost per day in cents.
    /// </summary>
    [JsonPropertyName("costPerDayCents")]
    public decimal CostPerDayCents { get; set; }

    /// <summary>
    /// The multi-region cost per day in cents.
    /// </summary>
    [JsonPropertyName("costPerDayMRCents")]
    public decimal CostPerDayMRCents { get; set; }

    /// <summary>
    /// The cost per day while the database is parked, in cents.
    /// </summary>
    [JsonPropertyName("costPerDayParkedCents")]
    public decimal CostPerDayParkedCents { get; set; }

    /// <summary>
    /// The cost per hour in cents.
    /// </summary>
    [JsonPropertyName("costPerHourCents")]
    public decimal CostPerHourCents { get; set; }

    /// <summary>
    /// The multi-region cost per hour in cents.
    /// </summary>
    [JsonPropertyName("costPerHourMRCents")]
    public decimal CostPerHourMRCents { get; set; }

    /// <summary>
    /// The cost per hour while the database is parked, in cents.
    /// </summary>
    [JsonPropertyName("costPerHourParkedCents")]
    public decimal CostPerHourParkedCents { get; set; }

    /// <summary>
    /// The cost per minute in cents.
    /// </summary>
    [JsonPropertyName("costPerMinCents")]
    public decimal CostPerMinCents { get; set; }

    /// <summary>
    /// The multi-region cost per minute in cents.
    /// </summary>
    [JsonPropertyName("costPerMinMRCents")]
    public decimal CostPerMinMRCents { get; set; }

    /// <summary>
    /// The cost per minute while the database is parked, in cents.
    /// </summary>
    [JsonPropertyName("costPerMinParkedCents")]
    public decimal CostPerMinParkedCents { get; set; }

    /// <summary>
    /// The cost per month in cents.
    /// </summary>
    [JsonPropertyName("costPerMonthCents")]
    public decimal CostPerMonthCents { get; set; }

    /// <summary>
    /// The multi-region cost per month in cents.
    /// </summary>
    [JsonPropertyName("costPerMonthMRCents")]
    public decimal CostPerMonthMRCents { get; set; }

    /// <summary>
    /// The cost per month while the database is parked, in cents.
    /// </summary>
    [JsonPropertyName("costPerMonthParkedCents")]
    public decimal CostPerMonthParkedCents { get; set; }

    /// <summary>
    /// The cost per GB of network transfer in cents.
    /// </summary>
    [JsonPropertyName("costPerNetworkGbCents")]
    public decimal CostPerNetworkGbCents { get; set; }

    /// <summary>
    /// The cost per GB of data read in cents.
    /// </summary>
    [JsonPropertyName("costPerReadGbCents")]
    public decimal CostPerReadGbCents { get; set; }

    /// <summary>
    /// The cost per GB of data written in cents.
    /// </summary>
    [JsonPropertyName("costPerWrittenGbCents")]
    public decimal CostPerWrittenGbCents { get; set; }
}

/// <summary>
/// Contains detailed configuration and topology information for an Astra DB database.
/// </summary>
public class RawDatabaseDetailsInfo
{
    /// <summary>
    /// The number of capacity units allocated to the database.
    /// </summary>
    [JsonPropertyName("capacityUnits")]
    public int CapacityUnits { get; set; }

    /// <summary>
    /// The cloud provider on which the database is deployed (e.g., AWS, GCP, AZURE).
    /// </summary>
    [JsonPropertyName("cloudProvider")]
    public string CloudProvider { get; set; }

    /// <summary>
    /// The list of datacenters associated with this database.
    /// </summary>
    [JsonPropertyName("datacenters")]
    public List<RawDatacenterInfo> Datacenters { get; set; }

    /// <summary>
    /// The database type (e.g., vector).
    /// </summary>
    [JsonPropertyName("dbType")]
    public string DbType { get; set; }

    /// <summary>
    /// The default keyspace for the database.
    /// </summary>
    [JsonPropertyName("keyspace")]
    public string Keyspace { get; set; }

    /// <summary>
    /// All keyspaces present in the database.
    /// </summary>
    [JsonPropertyName("keyspaces")]
    public List<string> Keyspaces { get; set; }

    /// <summary>
    /// The name of the database.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }

    /// <summary>
    /// The cloud region where the database is deployed.
    /// </summary>
    [JsonPropertyName("region")]
    public string Region { get; set; }

    /// <summary>
    /// The service tier of the database (e.g., serverless).
    /// </summary>
    [JsonPropertyName("tier")]
    public string Tier { get; set; }
}

/// <summary>
/// Contains information about a single datacenter associated with an Astra DB database.
/// </summary>
public class RawDatacenterInfo
{
    /// <summary>
    /// The number of capacity units in this datacenter.
    /// </summary>
    [JsonPropertyName("capacityUnits")]
    public int CapacityUnits { get; set; }

    /// <summary>
    /// The cloud provider for this datacenter.
    /// </summary>
    [JsonPropertyName("cloudProvider")]
    public string CloudProvider { get; set; }

    /// <summary>
    /// The date and time this datacenter was created.
    /// </summary>
    [JsonPropertyName("dateCreated")]
    public DateTime DateCreated { get; set; }

    /// <summary>
    /// The unique identifier of this datacenter.
    /// </summary>
    [JsonPropertyName("id")]
    public string Id { get; set; }

    /// <summary>
    /// A value indicating whether this is the primary datacenter.
    /// </summary>
    [JsonPropertyName("isPrimary")]
    public bool IsPrimary { get; set; }

    /// <summary>
    /// The name of this datacenter.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }
}

/// <summary>
/// Contains usage metrics for an Astra DB database.
/// </summary>
public class RawDatabaseMetrics
{
    /// <summary>
    /// The total number of errors recorded for this database.
    /// </summary>
    [JsonPropertyName("errorsTotalCount")]
    public long ErrorsTotalCount { get; set; }

    /// <summary>
    /// The size of live (active) data in bytes.
    /// </summary>
    [JsonPropertyName("liveDataSizeBytes")]
    public long LiveDataSizeBytes { get; set; }

    /// <summary>
    /// The total number of read requests made to this database.
    /// </summary>
    [JsonPropertyName("readRequestsTotalCount")]
    public long ReadRequestsTotalCount { get; set; }

    /// <summary>
    /// The total number of write requests made to this database.
    /// </summary>
    [JsonPropertyName("writeRequestsTotalCount")]
    public long WriteRequestsTotalCount { get; set; }
}

/// <summary>
/// Contains storage configuration details for an Astra DB database.
/// </summary>
public class RawDatabaseStorage
{
    /// <summary>
    /// The storage amount displayed to the user, in GB.
    /// </summary>
    [JsonPropertyName("displayStorage")]
    public int DisplayStorage { get; set; }

    /// <summary>
    /// The number of nodes in the database cluster.
    /// </summary>
    [JsonPropertyName("nodeCount")]
    public int NodeCount { get; set; }

    /// <summary>
    /// The replication factor for the database.
    /// </summary>
    [JsonPropertyName("replicationFactor")]
    public int ReplicationFactor { get; set; }

    /// <summary>
    /// The total storage allocated to the database, in GB.
    /// </summary>
    [JsonPropertyName("totalStorage")]
    public int TotalStorage { get; set; }
}
