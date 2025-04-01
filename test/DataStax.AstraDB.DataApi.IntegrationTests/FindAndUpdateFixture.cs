using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[CollectionDefinition("FindAndUpdate")]
public class FindAndUpdateCollection : ICollectionFixture<FindAndUpdateFixture>
{

}

public class FindAndUpdateFixture : IDisposable, IAsyncLifetime
{
    public DataApiClient Client { get; private set; }
    public Database Database { get; private set; }
    public string OpenAiApiKey { get; set; }

    public FindAndUpdateFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        var token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        var databaseUrl = configuration["URL"] ?? configuration["AstraDB:DatabaseUrl"];
        OpenAiApiKey = configuration["OPENAI_APIKEYNAME"];

        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger("../../../findandupdate_latest_run.log"));
        ILogger logger = factory.CreateLogger("IntegrationTests");

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug
        };
        Client = new DataApiClient(token, clientOptions, logger);
        Database = Client.GetDatabase(databaseUrl);
    }

    public async Task InitializeAsync()
    {
        await CreateUpdatesCollection();
        var collection = Database.GetCollection<SimpleObject>(_queryCollectionName);
        UpdatesCollection = collection;
    }

    public async Task DisposeAsync()
    {
        await Database.DropCollectionAsync(_queryCollectionName);
    }

    public Collection<SimpleObject> UpdatesCollection { get; private set; }


    private const string _queryCollectionName = "findAndUpdateCollection";
    private async Task CreateUpdatesCollection()
    {
        var collection = await CreateUpdatesCollection(_queryCollectionName);
        UpdatesCollection = collection;
    }

    internal async Task<Collection<SimpleObject>> CreateUpdatesCollection(string collectionName)
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
                        StringArrayProperty = new[] { "cat1", "cat2", "cat3" }
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
                        StringArrayProperty = new[] { "dog1", "dog2", "dog3" }
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
                        StringArrayProperty = new[] { "horse1", "horse2", "horse3" }
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
                        StringArrayProperty = new[] { "cow1", "cow2", "cow3" }
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
                        StringArrayProperty = new[] { "alligator1", "alligator2", "alligator3" }
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
                    StringArrayProperty = new[] { $"animal{i}", $"animal{100 + i}", $"animal{200 + i}" },
                    DateTimeProperty = new DateTime(2019, 5, i),
                }
            });
        }
        var collection = await Database.CreateCollectionAsync<SimpleObject>(collectionName);
        await collection.InsertManyAsync(items);

        return collection;
    }

    public void Dispose()
    {
        //nothing needed
    }
}