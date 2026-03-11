using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Logging;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

public class BaseFixture
{
    private readonly AssemblyFixture _assemblyFixture;

    public DataAPIClient Client { get; set; }
    public Database Database { get; set; }
    public DataAPIClient ClientWithoutToken { get; set; }
    public string DatabaseUrl { get; set; }
    public string Token { get; set; }
    public string Destination => _assemblyFixture.Destination;

    public BaseFixture(AssemblyFixture assemblyFixture, string fixtureName)
    {
        _assemblyFixture = assemblyFixture;
        Client = _assemblyFixture.CreateApiClient(fixtureName);
        DatabaseUrl = _assemblyFixture.DatabaseUrl;
        Client.Logger.LogInformation("Using Database URL: {DatabaseUrl}", DatabaseUrl);
        Database = Client.GetDatabase(DatabaseUrl);
        ClientWithoutToken = _assemblyFixture.CreateApiClient(fixtureName, false);
        Token = _assemblyFixture.Token;
    }

}