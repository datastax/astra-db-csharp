using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DataStax.AstraDB.DataAPI;
using DataStax.AstraDB.DataAPI.IntegrationTests.Tests;

namespace DataStax.AstraDB.DataAPI.IntegrationTests;

class Program
{
    static async Task Main(string[] args)
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

        var clientOptions = new DataAPIClientOptions();
        clientOptions.RunMode = DataStax.AstraDB.DataAPI.Core.RunMode.Debug;
        var client = new DataAPIClient(token, clientOptions, logger);
        var database = client.GetDatabase(databaseUrl);

        await CollectionTests.InsertDocumentAsync(database);
    }
}