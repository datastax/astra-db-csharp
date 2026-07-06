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
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("Database")]
public class DatabaseCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<DatabaseFixture>
{

}

public class DatabaseFixture : BaseFixture
{
    public DatabaseFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "database")
    {
    }
}