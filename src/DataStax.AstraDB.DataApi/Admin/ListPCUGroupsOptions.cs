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

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// Options for AstraDatabasesAdmin's ListPCUGroups command.
/// </summary>
public class ListPCUGroupsOptions : CommandOptions
{

    /// <summary>
    /// If set, filters results to this cloud provider only.
    /// Either both or none of CloudProvider and Region must be set
    /// </summary>
    public AstraDatabaseCloudProvider? CloudProvider { get; set; }

    /// <summary>
    /// If set, filters results to this region only.
    /// Either both or none of CloudProvider and Region must be set
    /// </summary>
    public string Region { get; set; }

    internal ListPCUGroupsOptions(CommandOptions source) : base(source)
    {
    }

    /// <summary>
    /// Creates a new instance of <see cref="ListPCUGroupsOptions"/> with default values.
    /// </summary>
    public ListPCUGroupsOptions() : base()
    {
    }

    static internal ListPCUGroupsOptions FromCommandOptions(
        CommandOptions options, AstraDatabaseCloudProvider? cloudProvider = null, string region = null
    )
    {
        if (options == null) return null;
        return new ListPCUGroupsOptions(options) {
            CloudProvider = cloudProvider, Region = region
        };
    }

}
