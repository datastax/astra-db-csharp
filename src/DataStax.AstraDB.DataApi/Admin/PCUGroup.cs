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
using System.Text.Json.Serialization;
using DataStax.AstraDB.DataApi.SerDes;

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// The specifications for a PCU (Provisioned Capacity Unit) group,
/// such as the ones returned when querying the DevOps API for PCU groups.
/// </summary>
public class PCUGroup
{
    /// <summary>
    /// The unique identifier for the PCU group (a UUID as a string).
    /// </summary>
    [JsonPropertyName("uuid")]
    public string Id { get; set; }

    /// <summary>
    /// The organization ID this PCU group belongs to.
    /// </summary>
    [JsonPropertyName("orgId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string OrgId { get; set; }

    /// <summary>
    /// The title (name) of the PCU group.
    /// </summary>
    [JsonPropertyName("title")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string Title { get; set; }

    /// <summary>
    /// The cloud provider for this PCU group (e.g. 'AWS').
    /// </summary>
    [JsonPropertyName("cloudProvider")]
    [JsonConverter(typeof(CloudProviderTypeConverter))]
    public CloudProviderType? CloudProvider { get; set; }

    /// <summary>
    /// The region this PCU group is ascribed to.
    /// </summary>
    [JsonPropertyName("region")]
    public string Region { get; set; }

    /// <summary>
    /// The instance type for this PCU group.
    /// </summary>
    [JsonPropertyName("instanceType")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string InstanceType { get; set; }

    /// <summary>
    /// The PCU type descriptor.
    /// </summary>
    [JsonPropertyName("pcuType")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public PCUType PCUType { get; set; }

    /// <summary>
    /// The provisioning type (e.g. 'shared').
    /// </summary>
    [JsonPropertyName("provisionType")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string ProvisionType { get; set; }

    /// <summary>
    /// The minimum shared hourly PCUs in the group.
    /// </summary>
    [JsonPropertyName("min")]
    public int Min { get; set; }

    /// <summary>
    /// The maximum shared hourly PCUs in the group.
    /// </summary>
    [JsonPropertyName("max")]
    public int Max { get; set; }

    /// <summary>
    /// The absolute required PCUs in the group.
    /// </summary>
    [JsonPropertyName("reserved")]
    public int Reserved { get; set; }

    /// <summary>
    /// A description of the PCU group.
    /// </summary>
    [JsonPropertyName("description")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string Description { get; set; }

    /// <summary>
    /// Creation time of the PCU group.
    /// </summary>
    [JsonPropertyName("createdAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? CreatedAt { get; set; }

    /// <summary>
    /// Update time of the PCU group.
    /// </summary>
    [JsonPropertyName("updatedAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? UpdatedAt { get; set; }

    /// <summary>
    /// Identifier of the user who created the PCU group.
    /// </summary>
    [JsonPropertyName("createdBy")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string CreatedBy { get; set; }

    /// <summary>
    /// Identifier of the user who updated the PCU group.
    /// </summary>
    [JsonPropertyName("updatedBy")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string UpdatedBy { get; set; }

    /// <summary>
    /// The current status of the PCU group (e.g. 'INITIALIZING').
    /// </summary>
    [JsonPropertyName("status")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string Status { get; set; }
}

/// <summary>
///  A PCU (Provisioned Capacity Unit) group type descriptor,
///  describing a specific PCU configuration available in a region.
/// </summary>
public class PCUType
{
    /// <summary>
    /// The type of PCU group (e.g. 'standard').
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; set; }

    /// <summary>
    /// The region where this PCU type is available.
    /// </summary>
    [JsonPropertyName("region")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string Region { get; set; }

    /// <summary>
    /// The cloud provider for this PCU type (e.g. 'AWS').
    /// </summary>
    [JsonPropertyName("provider")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonConverter(typeof(CloudProviderTypeConverter))]
    public CloudProviderType? CloudProvider { get; set; }

    /// <summary>
    /// Hardware specifications for this PCU type.
    /// </summary>
    [JsonPropertyName("details")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public PCUTypeDetails Details { get; set; }
}

/// <summary>
/// The details of a PCU (Provisioned Capacity Unit) group type,
/// describing the hardware specifications for a particular PCU configuration.
/// </summary>
public class PCUTypeDetails
{
    /// <summary>
    /// The number of virtual CPUs for this PCU type.
    /// </summary>
    [JsonPropertyName("vCPU")]
    public int VCpu { get; set; }

    /// <summary>
    /// The amount of memory for this PCU type.
    /// </summary>
    [JsonPropertyName("memory")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string Memory { get; set; }

    /// <summary>
    /// The amount of disk cache for this PCU type.
    /// </summary>
    [JsonPropertyName("disk_cache")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string DiskCache { get; set; }
}
