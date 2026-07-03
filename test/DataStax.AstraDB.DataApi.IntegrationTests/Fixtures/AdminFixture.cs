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
using DataStax.AstraDB.DataApi.Core;
using System.Text.RegularExpressions;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

public class AdminFixture : BaseFixture
{
    public string DatabaseName { get; set; }
    public Guid DatabaseId { get; set; }

    public AdminFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "admin")
    {
        DatabaseName = assemblyFixture.DatabaseName;
        var dbId = GetDatabaseIdFromUrl(assemblyFixture.DatabaseUrl);
        if (dbId != null)
        {
            DatabaseId = dbId.Value;
        }
    }

    public IDatabaseAdmin CreateAdmin(Database database = null)
    {
        return (database ?? Database).GetAdmin();
    }

    public static Guid? GetDatabaseIdFromUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url))
            return null;

        // Match the first UUID in the URL
        var match = Regex.Match(url, @"([0-9a-fA-F-]{36})");
        return match.Success ? Guid.Parse(match.Value) : null;
    }

}
