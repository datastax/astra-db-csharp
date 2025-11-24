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

public enum QueryDatabaseStates
{
    nonterminated,
    all,
    active,
    pending,
    preparing,
    prepared,
    initializing,
    parked,
    parking,
    unparking,
    terminating,
    terminated,
    resizing,
    error,
    maintenance,
    suspended,
    suspending
}

public enum QueryCloudProvider
{
    ALL,
    AWS,
    GCP,
    AZURE
}