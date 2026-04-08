using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

public class AssemblyFixture
{
    public string Token { get; private set; }
    public string AdminToken { get; private set; }
    public string OpenAiApiKey { get; private set; }

    public string DatabaseUrl { get; private set; }
    public string DatabaseName { get; private set; }
    public string Destination { get; private set; } = "astra";
    public string Environment { get; private set; } = "prod";

    public AssemblyFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        Token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        AdminToken = configuration["ADMINTOKEN"] ?? configuration["AstraDB:AdminToken"];
        OpenAiApiKey = configuration["OPENAI_APIKEYNAME"] ?? configuration["AstraDB:OpenAiApiKey"];
        DatabaseName = configuration["DATABASE_NAME"] ?? configuration["AstraDB:DatabaseName"];
        DatabaseUrl = configuration["URL"] ?? configuration["AstraDB:Url"];
        Destination = configuration["DESTINATION"] ?? configuration["AstraDB:Destination"] ?? "astra";
        Environment = configuration["ENVIRONMENT"] ?? configuration["AstraDB:Environment"] ?? "production";

    }

    public DataAPIClient CreateApiClient(string fixtureName, bool useToken = true)
    {
        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger($"../../../_logs/{fixtureName}_fixture_latest_run.log"));
        ILogger logger = factory.CreateLogger(fixtureName);

        logger.LogInformation("Database URL: {DatabaseUrl}", DatabaseUrl);
        logger.LogInformation("Database Destination: {Destination}", Destination);

        DataApiDestination? destination = DataApiDestination.ASTRA;
        switch (Destination?.ToLower())
        {
            case "astra":
                destination = DataApiDestination.ASTRA;
                break;
            case "dse":
                destination = DataApiDestination.DSE;
                break;
            case "hcd":
                destination = DataApiDestination.HCD;
                break;
            case "cassandra":
                destination = DataApiDestination.CASSANDRA;
                break;
            case "other":
                destination = DataApiDestination.OTHER;
                break;
            default:
                destination = DataApiDestination.ASTRA;
                break;
        }

        DBEnvironment? environment = DBEnvironment.Production;
        switch (Environment?.ToLower())
        {
            case "production":
            case "prod":
                environment = DBEnvironment.Production;
                break;
            case "testing":
            case "test":
                environment = DBEnvironment.Test;
                break;
            case "development":
            case "dev":
                environment = DBEnvironment.Dev;
                break;
            default:
                environment = DBEnvironment.Production;
                break;
        }

        logger.LogInformation("Using destination: {Destination}", destination.ToString());

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug,
            Destination = destination,
            Environment = environment,
        };

        return new DataAPIClient(useToken ? Token : null, clientOptions, logger);
    }

}