using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Reflection.Metadata;
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
        await CreateDeleteTable();
    }

    public async Task DisposeAsync()
    {
        await Database.DropTableAsync(_queryTableName);
        await Database.DropTableAsync(_deleteTableName);
    }

    public Table<RowBook> SearchTable { get; private set; }
    public Table<RowBook> DeleteTable { get; private set; }

    private const string _queryTableName = "tableQueryTests";
    private async Task CreateSearchTable()
    {
        try
        {
            var rows = new List<RowBook>() {
            new RowBook()
            {
                Title = "Computed Wilderness",
                Author = "Ryan Eau",
                NumberOfPages = 22,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = new HashSet<string>() { "History", "Biography" },
                Rating = 1.5f
            },
            new RowBook()
            {
                Title = "Desert Peace",
                Author = "Walter Dray",
                NumberOfPages = 33,
                DueDate = DateTime.Now - TimeSpan.FromDays(2),
                Genres = new HashSet<string>() { "Fiction" },
                Rating = 2.50123f
            }
        };
            for (var i = 0; i < 100; i++)
            {
                var row = new RowBook()
                {
                    Title = "Title " + i,
                    Author = "Author Number" + i,
                    NumberOfPages = 400 + i,
                    DueDate = DateTime.Now - TimeSpan.FromDays(1),
                    Genres = (i % 2 == 0)
                        ? new HashSet<string> { "History", "Biography" }
                        : new HashSet<string> { "Fiction", "History" },
                    Rating = (float)new Random().NextDouble()
                };
                rows.Add(row);
            }
            var table = await Database.CreateTableAsync<RowBook>(_queryTableName);
            await table.CreateIndexAsync(new TableIndex()
            {
                IndexName = "number_of_pages_index",
                Definition = new TableIndexDefinition<RowBook, int>()
                {
                    Column = (b) => b.NumberOfPages
                }
            });
            await table.CreateVectorIndexAsync(new TableVectorIndex()
            {
                IndexName = "author_index",
                Definition = new TableVectorIndexDefinition<RowBook, object>()
                {
                    Column = (b) => b.Author
                }
            });
            await table.CreateIndexAsync(new TableIndex()
            {
                IndexName = "due_date_index",
                Definition = new TableIndexDefinition<RowBook, DateTime>()
                {
                    Column = (b) => b.DueDate
                }
            });
            await table.InsertManyAsync(rows);
            SearchTable = table;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }

    private const string _deleteTableName = "tableDeleteTests";
    private async Task CreateDeleteTable()
    {
        var rows = new List<RowBook>();
        for (var i = 0; i < 10; i++)
        {
            var row = new RowBook()
            {
                Title = "Title " + i,
                Author = "Author Number" + i,
                NumberOfPages = 400 + i,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = (i % 2 == 0)
                    ? new HashSet<string> { "History", "Biography" }
                    : new HashSet<string> { "Fiction", "History" },
                Rating = (float)new Random().NextDouble()
            };
            rows.Add(row);
        }
        for (var i = 10; i < 20; i++)
        {
            var row = new RowBook()
            {
                Title = "Title " + i,
                Author = "AuthorDeleteMe",
                NumberOfPages = 22,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = (i % 2 == 0)
                    ? new HashSet<string> { "History", "Biography" }
                    : new HashSet<string> { "Fiction", "History" },
                Rating = (float)new Random().NextDouble()
            };
            rows.Add(row);
        }
        var table = await Database.CreateTableAsync<RowBook>(_deleteTableName);
        await table.CreateIndexAsync(new TableIndex()
        {
            IndexName = "delete_table_number_of_pages_index",
            Definition = new TableIndexDefinition<RowBook, int>()
            {
                Column = (b) => b.NumberOfPages
            }
        });
        await table.CreateVectorIndexAsync(new TableVectorIndex()
        {
            IndexName = "delete_table_author_index",
            Definition = new TableVectorIndexDefinition<RowBook, object>()
            {
                Column = (b) => b.Author
            }
        });
        await table.CreateIndexAsync(new TableIndex()
        {
            IndexName = "delete_table_due_date_index",
            Definition = new TableIndexDefinition<RowBook, DateTime>()
            {
                Column = (b) => b.DueDate
            }
        });
        await table.InsertManyAsync(rows);
        DeleteTable = table;
    }

    public void Dispose()
    {
        //nothing needed
    }
}