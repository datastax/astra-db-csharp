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
/// Options used for AstraDatabasesAdmin's GetDatabaseAdmin methods.
/// </summary>
public class GetDatabaseAdminOptions : CommandOptions
{

    /// <summary>
    /// Creates a new instance of <see cref="GetDatabaseAdminOptions"/> with default values.
    /// </summary>
    public GetDatabaseAdminOptions()
    {
    }

    private GetDatabaseAdminOptions(CommandOptions source) : base(source)
    {
    }

    static internal GetDatabaseAdminOptions FromCommandOptions(CommandOptions options)
    {
        return options == null ? null : new GetDatabaseAdminOptions(options);
    }

}
