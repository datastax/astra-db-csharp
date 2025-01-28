using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class ClientFixture : IDisposable
{
    public ClientFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        var token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        var databaseUrl = configuration["URL"] ?? configuration["AstraDB:DatabaseUrl"];

        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
        ILogger logger = factory.CreateLogger("IntegrationTests");

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug
        };
        Client = new DataApiClient(token, clientOptions, logger);
        Database = Client.GetDatabase(databaseUrl);
    }

    public void Dispose()
    {
        // ... clean up test data from the database ...
    }

    public DataApiClient Client { get; private set; }
    public Database Database { get; private set; }
}