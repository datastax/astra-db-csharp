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
/// Options used for DataAPIClient's GetAstraDatabasesAdmin methods.
/// </summary>
public class GetAstraDatabasesAdminOptions : CommandOptions
{
    /// <summary>
    /// Creates a new instance of <see cref="GetAstraDatabasesAdminOptions"/> with default values.
    /// </summary>
    public GetAstraDatabasesAdminOptions()
    {
    }

    private GetAstraDatabasesAdminOptions(CommandOptions source) : base(source)
    {
    }

    static internal GetAstraDatabasesAdminOptions FromCommandOptions(CommandOptions options)
    {
        return options == null ? null : new GetAstraDatabasesAdminOptions(options);
    }

}
