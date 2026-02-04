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
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// The metadata information for a region.
/// </summary>
public class RegionInfo
{
    /// <summary>
    /// The classification of the region.
    /// </summary>
    [JsonPropertyName("classification")]
    public string Classification { get; set; }

    /// <summary>
    /// The cloud provider of the region.
    /// </summary>
    [JsonPropertyName("cloudProvider")]
    public string CloudProvider { get; set; }

    /// <summary>
    /// The display name of the region.
    /// </summary>
    [JsonPropertyName("displayName")]
    public string DisplayName { get; set; }

    /// <summary>
    /// Indicates if the region is enabled.
    /// </summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    /// <summary>
    /// The name of the region.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }

    /// <summary>
    /// The type of the region.
    /// </summary>
    [JsonPropertyName("region_type")]
    public string RegionType { get; set; }

    /// <summary>
    /// Indicates if the region is reserved for qualified users.
    /// </summary>
    [JsonPropertyName("reservedForQualifiedUsers")]
    public bool ReservedForQualifiedUsers { get; set; }

    /// <summary>
    /// The zone of the region.
    /// </summary>
    [JsonPropertyName("zone")]
    public string Zone { get; set; }
}