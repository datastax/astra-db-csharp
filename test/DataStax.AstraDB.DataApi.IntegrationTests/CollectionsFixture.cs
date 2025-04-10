using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[CollectionDefinition("DatabaseAndCollections")]
public class DatabaseAndCollectionsCollection : ICollectionFixture<CollectionsFixture>
{

}

public class CollectionsFixture : IDisposable, IAsyncLifetime
{
    public DataApiClient Client { get; private set; }
    public Database Database { get; private set; }
    public string OpenAiApiKey { get; set; }
    public string DatabaseUrl { get; set; }

    public CollectionsFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        var token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        DatabaseUrl = configuration["URL"] ?? configuration["AstraDB:DatabaseUrl"];
        OpenAiApiKey = configuration["OPENAI_APIKEYNAME"];

        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger("../../../collections_fixture_latest_run.log"));
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
        await CreateSearchCollection();
        var collection = Database.GetCollection<SimpleObject>(_queryCollectionName);
        SearchCollection = collection;
    }

    public async Task DisposeAsync()
    {
        await Database.DropCollectionAsync(_queryCollectionName);
    }

    public Collection<SimpleObject> SearchCollection { get; private set; }


    private const string _queryCollectionName = "simpleObjectsQueryTests";
    private async Task CreateSearchCollection()
    {
        List<SimpleObject> items = new List<SimpleObject>() {
                new()
                {
                    _id = 0,
                    Name = "Cat",
                    Properties = new Properties() {
                        PropertyOne = "groupone",
                        PropertyTwo = "cat",
                        IntProperty = 1,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "cat1", "cat2", "cat3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 1, 0)
                    }
                },
                new()
                {
                    _id = 1,
                    Name = "Dog",
                    Properties = new Properties() {
                        PropertyOne = "groupone",
                        PropertyTwo = "dog",
                        IntProperty = 2,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "dog1", "dog2", "dog3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 2, 0)
                    }
                },
                new()
                {
                    _id = 2,
                    Name = "Horse",
                    Properties = new Properties() {
                        PropertyOne = "grouptwo",
                        PropertyTwo = "horse",
                        IntProperty = 3,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "horse1", "horse2", "horse3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 3, 0)
                    }
                },
                new()
                {
                    _id = 3,
                    Name = "Cow",
                    Properties = new Properties() {
                        PropertyOne = "grouptwo",
                        PropertyTwo = "cow",
                        IntProperty = 4,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "cow1", "cow2", "cow3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 4, 0)
                    }
                },
                new()
                {
                    _id = 4,
                    Name = "Alligator",
                    Properties = new Properties() {
                        PropertyOne = "grouptwo",
                        PropertyTwo = "alligator",
                        IntProperty = 5,
                        BoolProperty = true,
                        StringArrayProperty = new[] { "alligator1", "alligator2", "alligator3" },
                        DateTimeProperty = new DateTime(2020, 1, 1, 1, 5, 0)
                    }
                },
            };

        for (var i = 5; i <= 30; i++)
        {
            items.Add(new()
            {
                _id = i,
                Name = $"Animal{i}",
                Properties = new Properties()
                {
                    PropertyOne = "groupthree",
                    PropertyTwo = $"animal{i}",
                    IntProperty = i + 1,
                    BoolProperty = true,
                    StringArrayProperty = new[] { $"animal{i}1", $"animal{i}2" },
                    DateTimeProperty = new DateTime(2020, 1, 1, 1, i + 1, 0)
                }
            });
        }
        items.Add(new()
        {
            _id = 31,
            Name = "Cow Group 4",
            Properties = new Properties()
            {
                PropertyOne = "groupfour",
                PropertyTwo = "cow",
                IntProperty = 32,
                BoolProperty = true,
                StringArrayProperty = new[] { "cow1", "cow2" },
                DateTimeProperty = new DateTime(2020, 1, 1, 1, 32, 0)
            }
        });
        items.Add(new()
        {
            _id = 32,
            Name = "Alligator Group 4",
            Properties = new Properties()
            {
                PropertyOne = "groupfour",
                PropertyTwo = "alligator",
                IntProperty = 33,
                BoolProperty = true,
                StringArrayProperty = new[] { "alligator1", "alligator2" },
                DateTimeProperty = new DateTime(2020, 1, 1, 1, 33, 0)
            }
        });
        var collection = await Database.CreateCollectionAsync<SimpleObject>(_queryCollectionName);
        await collection.InsertManyAsync(items);

        SearchCollection = collection;
    }

    public void Dispose()
    {
        //nothing needed
    }
}