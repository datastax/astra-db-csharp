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

using DataStax.AstraDB.DataApi.Admin;
using System;
using System.Collections.Generic;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// The metadata information for a database.
/// </summary>
public class DatabaseInfo
{
    internal DatabaseInfo(RawDatabaseInfo rawInfo)
    {
        Id = rawInfo.Id;
        Name = rawInfo.Info.Name;
        OrgId = rawInfo.OrgId;
        OwnerId = rawInfo.OwnerId;
        Status = Enum.TryParse<AstraDatabaseStatus>(rawInfo.Status, true, out var status) ? status : AstraDatabaseStatus.UNKNOWN;
        CloudProvider = Enum.TryParse<AstraDatabaseCloudProvider>(rawInfo.Info.CloudProvider, true, out var cloudProvider) ? cloudProvider : AstraDatabaseCloudProvider.AWS;
        CreatedAt = rawInfo.CreationTime;
        LastUsed = rawInfo.LastUsageTime;
        Keyspaces = rawInfo.Info.Keyspaces;
        Region = rawInfo.Info.Region;
        RawDetails = rawInfo;
    }

    /// <summary>The unique identifier of the database.</summary>
    public string Id { get; set; }
    /// <summary>The name of the database.</summary>
    public string Name { get; set; }
    /// <summary>The organization ID that owns the database.</summary>
    public string OrgId { get; set; }
    /// <summary>The ID of the database owner.</summary>
    public string OwnerId { get; set; }
    /// <summary>The current lifecycle status of the database.</summary>
    public AstraDatabaseStatus Status { get; set; }
    /// <summary>The cloud provider where the database is deployed.</summary>
    public AstraDatabaseCloudProvider CloudProvider { get; set; }
    /// <summary>The date and time when the database was created.</summary>
    public DateTime CreatedAt { get; set; }
    /// <summary>The date and time when the database was last used.</summary>
    public DateTime LastUsed { get; set; }
    /// <summary>The list of keyspaces available in the database.</summary>
    public List<string> Keyspaces { get; set; } = new();
    /// <summary>The primary region where the database is deployed.</summary>
    public string Region { get; set; }
    /// <summary>The environment (e.g. "prod").</summary>
    public string Environment { get; set; } = "prod";
    /// <summary>The raw database details as returned by the Astra API.</summary>
    public RawDatabaseInfo RawDetails { get; set; }
}

/// <summary>
/// Regional deployment information for an Astra database.
/// </summary>
public class AstraDatabaseRegionInfo
{
    /// <summary>The API endpoint URL for this region.</summary>
    public string ApiEndpoint { get; set; }
    /// <summary>The date and time when the regional deployment was created.</summary>
    public DateTime CreatedAt { get; set; }
    /// <summary>The name of the region.</summary>
    public string Name { get; set; }
}

/// <summary>
/// The cloud provider where an Astra database is deployed.
/// </summary>
public enum AstraDatabaseCloudProvider
{
    /// <summary>Amazon Web Services.</summary>
    AWS,
    /// <summary>Google Cloud Platform.</summary>
    GCP,
    /// <summary>Microsoft Azure.</summary>
    AZURE
}

/// <summary>
/// The lifecycle status of an Astra database.
/// </summary>
public enum AstraDatabaseStatus
{
    /// <summary>The database is active and available.</summary>
    ACTIVE,
    /// <summary>The database is being associated with a resource.</summary>
    ASSOCIATING,
    /// <summary>The database is in an error state.</summary>
    ERROR,
    /// <summary>The database is being decommissioned.</summary>
    DECOMMISSIONING,
    /// <summary>The database is degraded.</summary>
    DEGRADED,
    /// <summary>The database is hibernated.</summary>
    HIBERNATED,
    /// <summary>The database is in the process of hibernating.</summary>
    HIBERNATING,
    /// <summary>The database is being initialized.</summary>
    INITIALIZING,
    /// <summary>The database is undergoing maintenance.</summary>
    MAINTENANCE,
    /// <summary>The database is parked (suspended).</summary>
    PARKED,
    /// <summary>The database is in the process of being parked.</summary>
    PARKING,
    /// <summary>The database is pending creation.</summary>
    PENDING,
    /// <summary>The database is prepared and ready to be activated.</summary>
    PREPARED,
    /// <summary>The database is being prepared.</summary>
    PREPARING,
    /// <summary>The database is being resized.</summary>
    RESIZING,
    /// <summary>The database is resuming from a hibernated state.</summary>
    RESUMING,
    /// <summary>The database has been terminated.</summary>
    TERMINATED,
    /// <summary>The database is in the process of being terminated.</summary>
    TERMINATING,
    /// <summary>The database status is unknown.</summary>
    UNKNOWN,
    /// <summary>The database is being unparked (resumed from suspension).</summary>
    UNPARKING,
    /// <summary>The database is synchronizing.</summary>
    SYNCHRONIZING,
}