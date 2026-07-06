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
using DataStax.AstraDB.DataApi.Tables;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("TableAlter")]
public class TableAlterCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<TableAlterFixture>
{

}

public class TableAlterFixture : BaseFixture
{
    public TableAlterFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "tableAlter")
    {
        
    }

    public async Task<Table<RowEventByDay>> CreateTestTable(string tableName)
    {
        var startDate = DateTime.UtcNow.Date.AddDays(7);

        var eventRows = new List<RowEventByDay>
        {
            new()
            {
                EventDate = startDate,
                Id = Guid.NewGuid(),
                Title = "Board Meeting",
                Location = "East Wing",
                Category = "administrative"
            },
            new()
            {
                EventDate = startDate.AddDays(1),
                Id = Guid.NewGuid(),
                Title = "Fire Drill",
                Location = "Building A",
                Category = "safety"
            },
            new()
            {
                EventDate = startDate.AddDays(2),
                Id = Guid.NewGuid(),
                Title = "Team Lunch",
                Location = "Cafeteria",
                Category = "social"
            }
        };


        var table = await Database.CreateTableAsync<RowEventByDay>(tableName);
        await table.InsertManyAsync(eventRows);

        return table;
    }

}