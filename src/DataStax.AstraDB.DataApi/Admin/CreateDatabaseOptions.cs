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
using System.Collections.Generic;
using System.Linq;

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// Options to use when creating a new database.
/// </summary>
public class CreateDatabaseOptions : BlockingCommandOptions
{
    private static readonly string[] NonVectorDBTypeStrings = { "nonvector", "non-vector", "non vector", "non_vector" };
    private const string DefaultTier = "serverless";
    private const int DefaultCapacityUnits = 1;
    private const string DefaultDBType = "vector";

    /// <summary>
    /// Name of the database to be created.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Which cloud provider should host the database?
    /// </summary>
    public CloudProviderType? CloudProvider { get; set; } = null;

    /// <summary>
    /// Database region.
    /// </summary>
    public string Region { get; set; }

    /// <summary>
    /// Name of the initial keyspace (defaults to "default_keyspace")
    /// </summary>
    public new string Keyspace
    {
        get => base.Keyspace;
        set => base.Keyspace = value;
    }

    /// <summary>
    /// Database tier (defaults to "serverless").
    /// </summary>
    public string Tier { get; set; } = DefaultTier;

    /// <summary>
    /// Capacity units for the database (defaults to 1).
    /// </summary>
    public int CapacityUnits { get; set; } = DefaultCapacityUnits;

    /// <summary>
    /// Database type (defaults to "vector").
    /// </summary>
    public string DBType { get; set; } = DefaultDBType;

    /// <summary>
    /// PCU group ID to use for provisioning the database. Optional.
    /// </summary>
    public string PCUGroupId { get; set; } = null;

    internal object ToPayload()
    {
        var payload = new Dictionary<string, object>();

        payload["tier"] = Tier;
        payload["capacityUnits"] = CapacityUnits;
        // explicit null DBType is passed into the payload (the DevOps API will error at that)
        if (DBType == null || !NonVectorDBTypeStrings.Contains(DBType.ToLower())) {
            payload["dbType"] = DBType;
        }
        if ( Name != null )
        {
            payload["name"] = Name;
        }
        if ( CloudProvider != null )
        {
            payload["cloudProvider"] = CloudProvider;
        }
        if ( Region != null )
        {
            payload["region"] = Region;
        }
        if ( Keyspace != null )
        {
            payload["keyspace"] = Keyspace;
        }
        if ( PCUGroupId != null )
        {
            payload["pcuGroupUUID"] = PCUGroupId;
        }

        return payload;
    }
}
