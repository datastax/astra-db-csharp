using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("TableIndexes")]
public class TableIndexesCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<TableIndexesFixture>
{
}

public class TableIndexesFixture : BaseFixture, IAsyncLifetime
{
    public TableIndexesFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "tableIndexes")
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

    public Table<RowEventByDay> FixtureTestTable { get; private set; }

    public async ValueTask InitializeAsync()
    {
        await CreateTestTable();
    }

    public async ValueTask DisposeAsync()
    {
        await Database.DropTableAsync(_fixtureTableName);
    }

    private const string _fixtureTableName = "tableIndexesTest";
    private async Task CreateTestTable()
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


        var table = await Database.CreateTableAsync<RowEventByDay>(_fixtureTableName);
        await table.InsertManyAsync(eventRows);

        FixtureTestTable = table;
    }

}