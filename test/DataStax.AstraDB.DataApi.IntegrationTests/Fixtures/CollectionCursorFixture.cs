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
    public Collection<CursorPaginationTestDocument> FilledPaginationCollection { get; private set; }
    public int FilledPaginationCollectionCount { get; private set; }

    public async ValueTask InitializeAsync()
    {
        await CreateFilledCollection();
        await CreateFilledPaginationCollection();
    }

    public async ValueTask DisposeAsync()
    {
        await Database.DropCollectionAsync(_collectionName);
        await Database.DropCollectionAsync(_paginationCollectionName);
    }

    private const string _collectionName = "collectionTestCursorFilled";
    private const string _paginationCollectionName = "collectionPaginationTestCursorFilled";
    private const int NUM_DOCS = 25;  // keep this between 21 and 39 (must be 1 full + 1 partial page in size)
    private const int NUM_DOCS_PAGINATION = 90;  // keep this above 2 * (2 * 20) and below 2 * (3 * 20)

    private async Task CreateFilledCollection()
    {

        // TODO replace with from-object creation when PR 136 gets merged and makes it into this branch
        var collectionDefinition = new CollectionDefinition
        {
            Vector = new VectorOptions { Dimension = 2 }
        };
        var collection = await Database.CreateCollectionAsync<CursorTestDocument>(_collectionName, collectionDefinition);

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

    private async Task CreateFilledPaginationCollection()
    {

        // TODO replace with from-object creation when PR 136 gets merged and makes it into this branch
        var collectionDefinition = new CollectionDefinition
        {
            Vector = new VectorOptions { Dimension = 2 }
        };
        var collection = await Database.CreateCollectionAsync<CursorPaginationTestDocument>(_paginationCollectionName, collectionDefinition);

        await collection.DeleteManyAsync(Builders<CursorPaginationTestDocument>.CollectionFilter.Empty());

        var testDocuments = new List<CursorPaginationTestDocument>();
        for (int i = 0; i < NUM_DOCS_PAGINATION; i++)
        {
            testDocuments.Add(new CursorPaginationTestDocument
            {
                id = i,
                text = $"doc number {i}",
                even = i % 2 == 0,
                vector = new float[] { i, 1.0f }
            });
        }
        
        await collection.InsertManyAsync(testDocuments);

        FilledPaginationCollection = collection;
        FilledPaginationCollectionCount = NUM_DOCS_PAGINATION;
    }

}
