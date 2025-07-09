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
        try
        {
            var keyspaces = Database.GetAdmin().ListKeyspaces();
            Console.WriteLine($"[Fixture] Connected. Keyspaces found: {keyspaces.Count()}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Fixture] Connection failed: {ex.Message}");
            throw;
        }
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