using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Tests;

[Collection("DatabaseAndCollections")]
public class DatabaseTests
{
    ClientFixture fixture;

    public DatabaseTests(ClientFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public void GetCollection_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => fixture.Database.GetCollection(null));
    }

    [Fact]
    public void GetCollection_ReturnsNotNull()
    {
        var collectionName = "collection";
        var collection = fixture.Database.GetCollection(collectionName);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
    }

    [Fact]
    public async Task CreateCollection_Simple()
    {
        var collectionName = "simpleCollection";
        var collection = await fixture.Database.CreateCollectionAsync(collectionName);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task CreateCollection_CancellationToken()
    {
        var collectionName = "simpleCollectionCanceled";
        var cts = new CancellationTokenSource();
        var commandOptions = new CommandOptions
        {
            CancellationToken = cts.Token
        };

        Collection<Document> collection = null;
        var task = Task.Run(async () =>
        {
            try
            {
                collection = await fixture.Database.CreateCollectionAsync(collectionName, commandOptions);
            }
            catch (OperationCanceledException)
            {
                // Optionally re-throw if you want the test to fail if it's caught later.
                throw; // Important for assertion!
            }
        }, cts.Token); // Pass the token to Task.Run as well
        cts.Cancel();
        Assert.Null(collection);
        var exists = await fixture.Database.DoesCollectionExistAsync(collectionName);
        Assert.False(exists);
    }

    [Fact]
    public async Task CreateCollection_WithDefaultIdType()
    {
        var collectionName = "withDefaultId";
        var options = new CollectionDefinition
        {
            DefaultId = new DefaultIdOptions
            {
                Type = DefaultIdType.ObjectId
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task CreateCollection_WithIndexingDeny()
    {
        var collectionName = "indexingDeny";
        var options = new CollectionDefinition
        {
            Indexing = new IndexingOptions
            {
                Deny = new List<string> { "blob" }
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task CreateCollection_WithIndexingAllow()
    {
        var collectionName = "indexingAllow";
        var options = new CollectionDefinition
        {
            Indexing = new IndexingOptions
            {
                Allow = new List<string> { "metadata" }
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task CreateCollection_WithVectorSimple()
    {
        var collectionName = "vectorSimple";
        var options = new CollectionDefinition
        {
            Vector = new VectorOptions
            {
                Dimension = 14,
                Metric = SimilarityMetric.DotProduct,
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task CreateCollection_WithVectorEuclidean()
    {
        var collectionName = "vectorEuclidean";
        var options = new CollectionDefinition
        {
            Vector = new VectorOptions
            {
                Dimension = 1536,
                Metric = SimilarityMetric.Euclidean
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task DoesCollectionExistAsync_ExistingCollection_ReturnsTrue()
    {
        var exists = await fixture.Database.DoesCollectionExistAsync(Constants.DefaultCollection);
        Assert.True(exists);
    }

    [Fact]
    public async Task DoesCollectionExistAsync_NonExistingCollection_ReturnsFalse()
    {
        var collectionName = "nonExistingCollection";
        var exists = await fixture.Database.DoesCollectionExistAsync(collectionName);
        Assert.False(exists);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DoesCollectionExistAsync_InvalidCollectionName_ReturnsFalse(string collectionName)
    {
        var exists = await fixture.Database.DoesCollectionExistAsync(collectionName);
        Assert.False(exists);
    }

    [Fact]
    public async Task ListCollectionNamesAsync_ShouldReturnCollectionNames()
    {
        var result = await fixture.Database.ListCollectionNamesAsync();
        Assert.NotNull(result);
        Assert.Contains(Constants.DefaultCollection, result.CollectionNames);
    }

    [Fact]
    public async Task ListCollectionNamesAsync_WithCommandOptions_ShouldReturnCollectionNames()
    {
        var commandOptions = new CommandOptions { /* Initialize with necessary options */ };
        var result = await fixture.Database.ListCollectionNamesAsync(commandOptions);
        Assert.NotNull(result);
        Assert.Contains(Constants.DefaultCollection, result.CollectionNames);
    }

    // [Fact]
    // public async Task CreateCollection_WithVectorizeHeader()
    // {
    //     var collectionName = "collectionVectorizeHeader";
    //     var options = new CollectionDefinition
    //     {
    //         Vector = new VectorOptions
    //         {
    //             Dimension = 1536,
    //             Metric = SimilarityMetric.DotProduct,
    //             Service = new VectorServiceOptions
    //             {
    //                 Provider = "openai",
    //                 ModelName = "text-embedding-ada-002",
    //                 Authentication = new Dictionary<string, object>
    //                 {
    //                     { "openai", "OPENAI_API_KEY" }
    //                 }
    //             }
    //         }
    //     };
    //     var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
    //     Assert.NotNull(collection);
    //     Assert.Equal(collectionName, collection.CollectionName);
    //     await fixture.Database.DropCollectionAsync(collectionName);
    // }

    // [Fact]
    // public async Task CreateCollection_WithVectorizeSharedKey()
    // {
    //     var collectionName = "collectionVectorizeSharedKey";
    //     var options = new CollectionDefinition
    //     {
    //         Vector = new VectorOptions
    //         {
    //             Dimension = 1536,
    //             Metric = SimilarityMetric.DotProduct,
    //             Service = new VectorServiceOptions
    //             {
    //                 Provider = "openai",
    //                 ModelName = "text-embedding-ada-002",
    //                 Authentication = new Dictionary<string, object>
    //                 {
    //                     { "openai", "OPENAI_API_KEY" }
    //                 }
    //             }
    //         }
    //     };
    //     var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
    //     Assert.NotNull(collection);
    //     Assert.Equal(collectionName, collection.CollectionName);
    //     await fixture.Database.DropCollectionAsync(collectionName);
    // }
}
