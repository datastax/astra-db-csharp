using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[CollectionDefinition("Tables")]
public class TablesCollection : ICollectionFixture<TablesFixture>
{

}

public class TablesFixture : IDisposable, IAsyncLifetime
{
    public DataApiClient Client { get; private set; }
    public Database Database { get; private set; }
    public string DatabaseUrl { get; set; }

    public TablesFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        var token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        DatabaseUrl = configuration["URL"] ?? configuration["AstraDB:DatabaseUrl"];

        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger("../../../tables_fixture_latest_run.log"));
        ILogger logger = factory.CreateLogger("IntegrationTests");

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug
        };
        Client = new DataApiClient(token, clientOptions, logger);
        Database = Client.GetDatabase(DatabaseUrl);
    }

    public async Task InitializeAsync()
    {
        await CreateSearchTable();
        //var table = Database.GetTable<RowTestObject>(_queryTableName);
        //SearchTable = table;
    }

    public async Task DisposeAsync()
    {
        await Database.DropTableAsync(_queryTableName);
    }

    public Table<RowBook> SearchTable { get; private set; }


    private const string _queryTableName = "tableQueryTests";
    private async Task CreateSearchTable()
    {
        var books = new List<RowBook>() {
            new RowBook()
            {
                Title = "Computed Wilderness",
                Author = "Ryan Eau",
                NumberOfPages = 432,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = new HashSet<string>() { "History", "Biography" },
                Rating = 4.5f
            },
            new RowBook()
            {
                Title = "Desert Peace",
                Author = "Walter Dray",
                NumberOfPages = 355,
                DueDate = DateTime.Now - TimeSpan.FromDays(2),
                Genres = new HashSet<string>() { "Fiction" },
                Rating = 2.50123f
            }
        };

        var table = await Database.CreateTableAsync<RowBook>(_queryTableName);
        await table.InsertManyAsync(books);

        SearchTable = table;
    }

    public void Dispose()
    {
        //nothing needed
    }
}