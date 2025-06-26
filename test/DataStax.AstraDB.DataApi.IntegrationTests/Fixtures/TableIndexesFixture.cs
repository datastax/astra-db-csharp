using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("TableIndexes")]
public class TableIndexesCollection : ICollectionFixture<TableIndexesFixture>
{

}

public class TableIndexesFixture : IDisposable, IAsyncLifetime
{
    public DataApiClient Client { get; private set; }
    public Database Database { get; private set; }
    public string DatabaseUrl { get; set; }

    public TableIndexesFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        var token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        DatabaseUrl = configuration["URL"] ?? configuration["AstraDB:DatabaseUrl"];

        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger("../../../_logs/table_indexes_fixture_latest_run.log"));
        ILogger logger = factory.CreateLogger("IntegrationTests");

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug
        };
        Client = new DataApiClient(token, clientOptions, logger);
        Database = Client.GetDatabase(DatabaseUrl);

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

    public async Task InitializeAsync()
    {
        await CreateTestTable();
    }

    public async Task DisposeAsync()
    {
        await Database.DropTableAsync(_fixtureTableName);
    }

    public Table<RowEventByDay> FixtureTestTable { get; private set; }


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

    public void Dispose()
    {
        // nothing needed
    }
}