using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("CollectionCursor")]
public class CollectionCursorCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<CollectionCursorFixture>
{
}

public class CollectionCursorFixture : BaseFixture, IAsyncLifetime
{
    public CollectionCursorFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "collectionCursor")
    {
    }

    public Collection<CursorTestDocument> FilledCollection { get; private set; }
    public int FilledCollectionCount { get; private set; }

    public async ValueTask InitializeAsync()
    {
        await CreateFilledCollection();
    }

    public async ValueTask DisposeAsync()
    {
        // TODO uncomment me
        //await Database.DropCollectionAsync(_collectionName);
    }

    private const string _collectionName = "collectionTestCursorFilled";
    private const int NUM_DOCS = 25;  // keep this between 21 and 39 (must be 1 full + 1 partial page in size)

    private async Task CreateFilledCollection()
    {

        // TODO replace with from-object creation when PR 125 gets merged and makes it into this branch
        var collectionDefinition = new CollectionDefinition
        {
            Vector = new VectorOptions { Dimension = 2 }
        };
        var collection = await Database.CreateCollectionAsync<CursorTestDocument>(_collectionName, collectionDefinition);

        // TODO uncomment this (now I'm iterating and go for a fast testing cycle with the data already there)
        await collection.DeleteManyAsync(Builders<CursorTestDocument>.CollectionFilter.Empty());

        var testDocuments = new List<CursorTestDocument>();
        for (int i = 0; i < NUM_DOCS; i++)
        {
            testDocuments.Add(new CursorTestDocument
            {
                Id = $"doc_{i + 1}",
                PText = "pA",
                PInt = i,
                Vector = new float[] { i, 1.0f }
            });
        }
        
        await collection.InsertManyAsync(testDocuments);
        
        FilledCollection = collection;
        FilledCollectionCount = NUM_DOCS;
    }
}
