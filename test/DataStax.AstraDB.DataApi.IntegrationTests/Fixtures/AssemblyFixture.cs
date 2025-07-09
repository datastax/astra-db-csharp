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
    }

    public DataApiClient CreateApiClient(string fixtureName, bool useToken = true)
    {
        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger($"../../../_logs/{fixtureName}_fixture_latest_run.log"));
        ILogger logger = factory.CreateLogger(fixtureName);

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug
        };

        return new DataApiClient(useToken ? Token : null, clientOptions, logger);
    }

}