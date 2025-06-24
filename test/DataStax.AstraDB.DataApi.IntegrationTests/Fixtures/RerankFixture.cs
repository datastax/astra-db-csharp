using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.SerDes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json.Serialization;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("RerankCollection")]
public class RerankCollection : ICollectionFixture<RerankFixture>
{

}

public class RerankFixture : IDisposable, IAsyncLifetime
{
    private const string _queryCollectionName = "rerankQueryTests";

    public DataApiClient Client { get; private set; }
    public Database Database { get; private set; }
    public string OpenAiApiKey { get; set; }
    public string DatabaseUrl { get; set; }
    public string Token { get; set; }
    public Collection<HybridSearchTestObject> HybridSearchCollection { get; private set; }

    public RerankFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables(prefix: "ASTRA_DB_")
            .Build();

        Token = configuration["TOKEN"] ?? configuration["AstraDB:Token"];
        DatabaseUrl = configuration["URL"] ?? configuration["AstraDB:DatabaseUrl"];
        OpenAiApiKey = configuration["OPENAI_APIKEYNAME"];

        using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger("../../../_logs/rerank_fixture_latest_run.log"));
        ILogger logger = factory.CreateLogger("IntegrationTests");

        var clientOptions = new CommandOptions
        {
            RunMode = RunMode.Debug
        };
        Client = new DataApiClient(Token, clientOptions, logger);
        Database = Client.GetDatabase(DatabaseUrl);
    }

    public async Task InitializeAsync()
    {
        await CreateSearchCollection();
        var collection = Database.GetCollection<HybridSearchTestObject>(_queryCollectionName);
        HybridSearchCollection = collection;
    }

    public async Task DisposeAsync()
    {
        await Database.DropCollectionAsync(_queryCollectionName);
    }

    private async Task CreateSearchCollection()
    {
        List<HybridSearchTestObject> items = new() {
            new()
            {
                Name = "Cat",
                HybridTest = "this animal is a cat"
            },
            new()
            {
                Name = "NotCat",
                VectorizeTest = "not cats are like cats but aren't actually cats",
                LexicalTest = "this animal is not a cat"
            },
            new()
            {
                Name = "Horse",
                HybridTest = "this animal is a horse"
            },
            new()
            {
                Name = "Cow",
                LexicalTest = "this animal is a cow, it is not a cat",
                VectorizeTest = "this animal is a cow, which has 4 legs like a cat does"
            }
        };

        var definition = new CollectionDefinition()
        {
            Lexical = new LexicalOptions()
            {
                Analyzer = new AnalyzerOptions()
                {
                    Tokenizer = new TokenizerOptions()
                    {
                        Name = "standard",
                        Arguments = new Dictionary<string, object>() { }
                    },
                    Filters = new List<string>()
                    {
                        "lowercase",
                        "stop",
                        "porterstem",
                        "asciifolding"
                    },
                    CharacterFilters = new List<string>() { }
                },
                Enabled = true
            },
            Rerank = new RerankOptions()
            {
                Enabled = true,
                Service = new RerankServiceOptions()
                {
                    ModelName = "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                    Provider = "nvidia"
                }
            },
            Vector = new VectorOptions()
            {
                Dimension = 1024,
                Metric = SimilarityMetric.Cosine,
                Service = new VectorServiceOptions()
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }
            }
        };

        var collection = await Database.CreateCollectionAsync<HybridSearchTestObject>(_queryCollectionName, definition);
        await collection.InsertManyAsync(items);

        HybridSearchCollection = collection;
    }

    public void Dispose()
    {
        //nothing needed
    }
}

public class HybridSearchTestObject
{
    //TODO: Ucomment when HybridSearch supports uuids
    //[DocumentId(DefaultIdType.Uuid)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Guid? Id { get; set; }
    public string Name { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [DocumentMapping(DocumentMappingField.Lexical)]
    public string LexicalTest { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [DocumentMapping(DocumentMappingField.Vectorize)]
    public string VectorizeTest { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [DocumentMapping(DocumentMappingField.Hybrid)]
    public string HybridTest { get; set; }
}