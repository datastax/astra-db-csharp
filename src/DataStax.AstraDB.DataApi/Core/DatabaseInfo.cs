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
        Status = Enum.TryParse<AstraDatabaseStatus>(rawInfo.Status, true, out var status) ? status : AstraDatabaseStatus.ERROR;
        CloudProvider = Enum.TryParse<AstraDatabaseCloudProvider>(rawInfo.Info.CloudProvider, true, out var cloudProvider) ? cloudProvider : AstraDatabaseCloudProvider.AWS;
        CreatedAt = rawInfo.CreationTime;
        LastUsed = rawInfo.LastUsageTime;
        Keyspaces = rawInfo.Info.Keyspaces;
        RawDetails = rawInfo;
    }

    public string Id { get; set; }
    public string Name { get; set; }
    public string OrgId { get; set; }
    public string OwnerId { get; set; }
    public AstraDatabaseStatus Status { get; set; }
    public AstraDatabaseCloudProvider CloudProvider { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime LastUsed { get; set; }
    public List<string> Keyspaces { get; set; } = new();
    public List<AstraDatabaseRegionInfo> Regions { get; set; }
    public string Environment { get; set; } = "prod";
    public RawDatabaseInfo RawDetails { get; set; }
}

public class AstraDatabaseRegionInfo
{
    public string ApiEndpoint { get; set; }
    public DateTime CreatedAt { get; set; }
    public string Name { get; set; }
}

public enum AstraDatabaseCloudProvider
{
    AWS,
    GCP,
    AZURE
}

public enum AstraDatabaseStatus
{
    ACTIVE,
    ERROR,
    DECOMMISSIONING,
    DEGRADED,
    HIBERNATED,
    HIBERNATING,
    INITIALIZING,
    MAINTENANCE,
    PARKED,
    PARKING,
    PENDING,
    PREPARED,
    PREPARING,
    RESIZING,
    RESUMING,
    TERMINATED,
    TERMINATING,
    UNKNOWN,
    UNPARKING,
    SYNCHRONIZING
}