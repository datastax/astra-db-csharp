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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// While timeouts can be set on a per-request basis (via the <see cref="CommandOptions.TimeoutOptions"/>), 
/// when the timeouts are not specified, the default timeouts will be used. These can also be overridden
/// by setting the <see cref="CommandOptions.TimeoutOptions.Defaults"/> property at any level in the command
/// hierarchy (e.g. <see cref="DataApiClient"/>, <see cref="Database"/>, <see cref="Collections.Collection"/>).
/// </summary>
/// <example>
/// <code>
/// The following example shows how to override the default timeouts at the client level.
/// Let's change the defaults for each request, as well as those for collection administration operations,
/// but leave the rest of the defaults unchanged.
/// var client = new DataApiClient(new CommandOptions
/// {
///     TimeoutOptions = new TimeoutOptions
///     {
///         Defaults = new TimeoutDefaults
///         {
///             RequestTimeout = TimeSpan.FromSeconds(45),
///             CollectionAdminTimeout = TimeSpan.FromMinutes(3),
///         }
///     }
/// });
/// </code>
/// </example>
/// <example>
/// The following example shows how to override the default timeouts at the collection level.
/// Let's change the defaults for each request, as well as those for collection administration operations,
/// and connections to the API, but leave the rest of the defaults unchanged.
/// var collection = client.GetDatabase("mydb").GetCollection("myCollection", new DatabaseCommandOptions
/// {
///     TimeoutOptions = new TimeoutOptions
///     {
///         Defaults = new TimeoutDefaults
///         {
///             ConnectionTimeout = TimeSpan.FromSeconds(3),
///             RequestTimeout = TimeSpan.FromMinutes(1),
///             CollectionAdminTimeout = TimeSpan.FromMinutes(5),
///         }
///     }
/// });
/// </example>
public class TimeoutDefaults
{
    /// <summary>
    /// 10 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(10);
    /// <summary>
    /// 5 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultConnectionTimeout = TimeSpan.FromSeconds(5);
    /// <summary>
    /// 30 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultBulkOperationTimeout = TimeSpan.FromSeconds(30);
    /// <summary>
    /// 60 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultCollectionAdminTimeout = TimeSpan.FromSeconds(60);
    /// <summary>
    /// 30 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultTableAdminTimeout = TimeSpan.FromSeconds(30);
    /// <summary>
    /// 10 minutes.
    /// </summary>
    public static readonly TimeSpan DefaultDatabaseAdminTimeout = TimeSpan.FromMinutes(10);
    /// <summary>
    /// 60 seconds.
    /// </summary>
    public static readonly TimeSpan DefaultKeyspaceAdminTimeout = TimeSpan.FromSeconds(60);

    /// <summary>
    /// The default timeout for a single request to the API.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; }
    /// <summary>
    /// The default timeout for establishing a connection to the API.
    /// </summary>
    public TimeSpan ConnectionTimeout { get; set; }
    /// <summary>
    /// The default timeout for bulk operations that involve multiple requests to the API (i.e. InsertMany)
    /// </summary>
    public TimeSpan BulkOperationTimeout { get; set; }
    /// <summary>
    /// The default timeout for collection administration operations, such as creating or deleting collections.
    /// </summary>
    public TimeSpan CollectionAdminTimeout { get; set; }
    /// <summary>
    /// The default timeout for table administration operations, such as creating or deleting tables.
    /// </summary>
    public TimeSpan TableAdminTimeout { get; set; }
    /// <summary>
    /// The default timeout for database administration operations, such as creating or deleting databases.
    /// </summary>
    public TimeSpan DatabaseAdminTimeout { get; set; }
    /// <summary>
    /// The default timeout for keyspace administration operations, such as creating or deleting keyspaces.
    /// </summary>
    public TimeSpan KeyspaceAdminTimeout { get; set; }
}

/// <summary>
/// An options class that allows you to override the timeouts for the interactions with the Data API.
/// </summary>
public class TimeoutOptions
{
    /// <summary>
    /// The timeout for establishing a connection to the API.
    /// </summary>
    public TimeSpan? ConnectionTimeout { get; set; }
    /// <summary>
    /// The timeout for individual requests to the API.
    /// </summary>
    public TimeSpan? RequestTimeout { get; set; }
    /// <summary>
    /// The timeout for bulk operations that involve multiple requests to the API.
    /// </summary>
    public TimeSpan? BulkOperationTimeout { get; set; }
    /// <summary>
    /// Defaults used for operations for which timeouts are not specified.
    /// See <see cref="TimeoutDefaults"/> for more details. 
    /// </summary>
    public TimeoutDefaults Defaults { get; set; }
}

