using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;

[CollectionDefinition("DatabaseAndCollections")]
public class DatabaseAndCollectionsCollection : ICollectionFixture<ClientFixture>
{

}

public class ClientFixture : IDisposable //, IAsyncLifetime
{
    public DataApiClient Client { get; private set; }
    public Database Database { get; private set; }
    public string OpenAiApiKey { get; set; }

    public ClientFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        var token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        var databaseUrl = configuration["URL"] ?? configuration["AstraDB:DatabaseUrl"];
        OpenAiApiKey = configuration["OPENAI_APIKEYNAME"];

        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger("../../../latest_run.log"));
        ILogger logger = factory.CreateLogger("IntegrationTests");

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug
        };
        Client = new DataApiClient(token, clientOptions, logger);
        Database = Client.GetDatabase(databaseUrl);

    }

    // public async Task InitializeAsync()
    // {
    //     //await Database.CreateCollectionAsync(Constants.DefaultCollection);
    // }

    // public async Task DisposeAsync()
    // {
    //     //await Database.DropCollectionAsync(Constants.DefaultCollection);
    // }

    public void Dispose()
    {

    }
}