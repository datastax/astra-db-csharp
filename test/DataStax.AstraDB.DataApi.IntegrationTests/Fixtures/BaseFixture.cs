using DataStax.AstraDB.DataApi.Core;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

public class BaseFixture
{
    private readonly AssemblyFixture _assemblyFixture;

    public DataApiClient Client { get; set; }
    public Database Database { get; set; }
    public DataApiClient ClientWithoutToken { get; set; }
    public string DatabaseUrl { get; set; }
    public string Token { get; set; }

    public BaseFixture(AssemblyFixture assemblyFixture, string fixtureName)
    {
        _assemblyFixture = assemblyFixture;
        Client = _assemblyFixture.CreateApiClient(fixtureName);
        DatabaseUrl = _assemblyFixture.DatabaseUrl;
        Database = Client.GetDatabase(DatabaseUrl);
        ClientWithoutToken = _assemblyFixture.CreateApiClient(fixtureName, false);
        Token = _assemblyFixture.Token;
    }

}