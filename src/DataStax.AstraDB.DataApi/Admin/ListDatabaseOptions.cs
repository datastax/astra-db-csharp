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
/// Options used for ListDatabasesAsync.
/// </summary>
public class ListDatabaseOptions
{
    /// <summary>
    /// Filter databases based on specific states.
    /// </summary>
    [JsonPropertyName("include")]
    public QueryDatabaseStates StatesToInclude { get; set; } = QueryDatabaseStates.nonterminated;

    /// <summary>
    /// Filter databases based on cloud provider.
    /// </summary>
    [JsonPropertyName("cloudProvider")]
    public QueryCloudProvider Provider { get; set; } = QueryCloudProvider.ALL;

    /// <summary>
    /// See <see cref="PageSizeLimit"/>. If getting an additional page of data, pass in the id of the last database in the previous page. 
    /// </summary>
    [JsonPropertyName("starting_after")]
    internal string StartingAfter { get; set; }

    /// <summary>
    /// Number of items to return "per page".
    /// </summary>
    [JsonPropertyName("limit")]
    public int PageSizeLimit = 100;
}

/// <summary>
/// Specifies the database state filter to apply when listing Astra DB databases.
/// </summary>
public enum QueryDatabaseStates
{
    /// <summary>All databases that have not been terminated.</summary>
    nonterminated,
    /// <summary>All databases regardless of state.</summary>
    all,
    /// <summary>Databases that are active and available.</summary>
    active,
    /// <summary>Databases waiting to be provisioned.</summary>
    pending,
    /// <summary>Databases being prepared for use.</summary>
    preparing,
    /// <summary>Databases that have been prepared and are ready to activate.</summary>
    prepared,
    /// <summary>Databases that are initializing.</summary>
    initializing,
    /// <summary>Databases that have been parked (suspended to save resources).</summary>
    parked,
    /// <summary>Databases in the process of being parked.</summary>
    parking,
    /// <summary>Databases in the process of being unparked (resumed).</summary>
    unparking,
    /// <summary>Databases in the process of being terminated.</summary>
    terminating,
    /// <summary>Databases that have been permanently terminated.</summary>
    terminated,
    /// <summary>Databases being resized.</summary>
    resizing,
    /// <summary>Databases that are in an error state.</summary>
    error,
    /// <summary>Databases undergoing maintenance.</summary>
    maintenance,
    /// <summary>Databases that have been suspended.</summary>
    suspended,
    /// <summary>Databases in the process of being suspended.</summary>
    suspending
}

/// <summary>
/// Specifies the cloud provider filter to apply when listing Astra DB databases.
/// </summary>
public enum QueryCloudProvider
{
    /// <summary>All cloud providers.</summary>
    ALL,
    /// <summary>Amazon Web Services.</summary>
    AWS,
    /// <summary>Google Cloud Platform.</summary>
    GCP,
    /// <summary>Microsoft Azure.</summary>
    AZURE
}