using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("CollectionFARRCursor")]
public class CollectionFARRCursorCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<CollectionFARRCursorFixture>
{
}

public class CollectionFARRCursorFixture : BaseFixture, IAsyncLifetime
{
    public CollectionFARRCursorFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "collectionFARRCursor")
    {
    }

    public Collection<FARRCursorTestVectorDocument> FilledVectorCollection { get; private set; }
    public Collection<FARRCursorTestVectorizeDocument> FilledVectorizeCollection { get; private set; }
    public int FilledVectorCollectionCount { get; private set; }
    public int FilledVectorizeCollectionCount { get; private set; }

    private bool IsAstra()
    {
        var destination = Database.Client.ClientOptions.Destination;
        return destination == DataAPIDestination.ASTRA;
    }

    public async ValueTask InitializeAsync()
    {
        if (IsAstra()) {
            await CreateFilledVectorCollection();
            await CreateFilledVectorizeCollection();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (IsAstra()) {
            await Database.DropCollectionAsync<FARRCursorTestVectorDocument>();
            await Database.DropCollectionAsync<FARRCursorTestVectorizeDocument>();
        }
    }

    private const int NUM_DOCS = 5;

    private async Task CreateFilledVectorCollection()
    {

        var collectionDefinition = new CollectionDefinition
        {
            Lexical = new LexicalOptions
            {
                Analyzer = new AnalyzerOptions
                {
                    Tokenizer = new TokenizerOptions
                    {
                        Name = "standard"
                    }
                }
            },
            Rerank = new RerankOptions
            {
                Enabled = true,
                Service = new RerankServiceOptions
                {
                    ModelName = "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                    Provider = "nvidia"
                }
            }
        };
        var collection = await Database.CreateCollectionAsync<FARRCursorTestVectorDocument>(collectionDefinition);

        await collection.DeleteManyAsync(Builders<FARRCursorTestVectorDocument>.CollectionFilter.Empty());

        var testDocuments = new List<FARRCursorTestVectorDocument>();
        for (int i = 0; i < NUM_DOCS; i++)
        {
            testDocuments.Add(new FARRCursorTestVectorDocument
            {
                Id = $"doc_{i + 1}",
                PText = $"text of document number {i + 1}",
                PInt = i + 1,
                Vector = new float[] { 1.0f, 1.0f / (i + 1) }
            });
        }
        
        await collection.InsertManyAsync(testDocuments);

        FilledVectorCollection = collection;
        FilledVectorCollectionCount = NUM_DOCS;
    }

    private async Task CreateFilledVectorizeCollection()
    {

        var collectionDefinition = new CollectionDefinition
        {
            Lexical = new LexicalOptions
            {
                Analyzer = new AnalyzerOptions
                {
                    Tokenizer = new TokenizerOptions
                    {
                        Name = "standard"
                    }
                }
            },
            Rerank = new RerankOptions
            {
                Enabled = true,
                Service = new RerankServiceOptions
                {
                    ModelName = "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                    Provider = "nvidia"
                }
            }
        };
        var collection = await Database.CreateCollectionAsync<FARRCursorTestVectorizeDocument>(collectionDefinition);

        await collection.DeleteManyAsync(Builders<FARRCursorTestVectorizeDocument>.CollectionFilter.Empty());

        var testDocuments = new List<FARRCursorTestVectorizeDocument>();
        for (int i = 0; i < NUM_DOCS; i++)
        {
            testDocuments.Add(new FARRCursorTestVectorizeDocument
            {
                Id = $"doc_{i + 1}",
                PText = $"text of document number {i + 1}",
                PInt = i + 1,
            });
        }
        
        await collection.InsertManyAsync(testDocuments);

        FilledVectorizeCollection = collection;
        FilledVectorizeCollectionCount = NUM_DOCS;
    }

}
