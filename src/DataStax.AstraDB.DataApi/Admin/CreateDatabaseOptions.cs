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

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// Options to use when creating a new database.
/// </summary>
public class CreateDatabaseOptions : BlockingCommandOptions
{
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

    internal object ToPayload()
    {
        var payload = new Dictionary<string, object>();

        // hardcoded properties
        payload["tier"] = "serverless";
        payload["capacityUnits"] = 1;
        payload["dbType"] = "vector";
        // specified properties
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

        return payload;
    }
}
