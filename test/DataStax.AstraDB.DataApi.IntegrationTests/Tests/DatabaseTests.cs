using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("DatabaseAndCollections")]
public class DatabaseTests
{
    CollectionsFixture fixture;

    public DatabaseTests(CollectionsFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task KeyspaceTests()
    {
        var collectionName = "simpleTestKeyspaceCollection";
        var keyspaceName = "simpleTestKeyspace";
        var databaseWithKeyspace = fixture.Client.GetDatabase(fixture.DatabaseUrl, keyspaceName);
        var admin = databaseWithKeyspace.GetAdmin();
        try
        {
            var dbOptions = new DatabaseCommandOptions
            {
                Keyspace = keyspaceName
            };
            await Assert.ThrowsAnyAsync<Exception>(async () => await fixture.Database.CreateCollectionAsync(collectionName, dbOptions));
            //create keyspace

            await admin.CreateKeyspaceAsync(keyspaceName);
            Thread.Sleep(30 * 1000);
            //passed-in dboptions should override keyspace and creation should work
            await fixture.Database.CreateCollectionAsync(collectionName, dbOptions);
            var collections = await databaseWithKeyspace.ListCollectionsAsync();
            var collection = collections.FirstOrDefault(c => c.Name == collectionName);
            Assert.NotNull(collection);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
            if (admin != null)
            {
                await admin.DropKeyspaceAsync(keyspaceName);
            }
        }
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
        var commandOptions = new DatabaseCommandOptions
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
                throw;
            }
        }, cts.Token);
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
        await fixture.Database.CreateCollectionAsync(Constants.DefaultCollection);
        var exists = await fixture.Database.DoesCollectionExistAsync(Constants.DefaultCollection);
        Assert.True(exists);
        await fixture.Database.DropCollectionAsync(Constants.DefaultCollection);
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
    public async Task Create_Drop_FlowWorks()
    {
        const string collectionName = "createAndDropCollection";
        await fixture.Database.CreateCollectionAsync(collectionName);
        var exists = await fixture.Database.DoesCollectionExistAsync(collectionName);
        Assert.True(exists);
        await fixture.Database.DropCollectionAsync(collectionName);
        exists = await fixture.Database.DoesCollectionExistAsync(collectionName);
        Assert.False(exists);
    }

    [Fact]
    public async Task ListCollectionNamesAsync_ShouldReturnCollectionNames()
    {
        await fixture.Database.CreateCollectionAsync(Constants.DefaultCollection);
        var result = await fixture.Database.ListCollectionNamesAsync();
        Assert.NotNull(result);
        Assert.Contains(Constants.DefaultCollection, result);
        await fixture.Database.DropCollectionAsync(Constants.DefaultCollection);
    }

    [Fact]
    public async Task ListCollectionNamesAsync_WithCommandOptions_ShouldReturnCollectionNames()
    {
        await fixture.Database.CreateCollectionAsync(Constants.DefaultCollection);
        var commandOptions = new DatabaseCommandOptions { /* Initialize with necessary options */ };
        var result = await fixture.Database.ListCollectionNamesAsync(commandOptions);
        Assert.NotNull(result);
        Assert.Contains(Constants.DefaultCollection, result);
        await fixture.Database.DropCollectionAsync(Constants.DefaultCollection);
    }

    [Fact]
    public async Task CreateCollection_WithVectorizeHeader()
    {
        var collectionName = "collectionVectorizeHeader";
        var options = new CollectionDefinition
        {
            Vector = new VectorOptions
            {
                Dimension = 1536,
                Metric = SimilarityMetric.DotProduct,
                Service = new VectorServiceOptions
                {
                    Provider = "openai",
                    ModelName = "text-embedding-ada-002",
                    Authentication = new Dictionary<string, object>
                    {
                        { "providerKey", fixture.OpenAiApiKey }
                    }
                }
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task CreateCollection_WithVectorizeSharedKey()
    {
        var collectionName = "collectionVectorizeSharedKey";
        var options = new CollectionDefinition
        {
            Vector = new VectorOptions
            {
                Dimension = 1536,
                Metric = SimilarityMetric.DotProduct,
                Service = new VectorServiceOptions
                {
                    Provider = "openai",
                    ModelName = "text-embedding-ada-002",
                    Authentication = new Dictionary<string, object>
                    {
                        { "providerKey", fixture.OpenAiApiKey }
                    }
                }
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task GetCollectionMetadata()
    {
        var collectionName = "collectionMetadataTest";
        try
        {
            var options = new CollectionDefinition
            {
                Indexing = new IndexingOptions
                {
                    Allow = new List<string> { "metadata" }
                },
                Vector = new VectorOptions
                {
                    Dimension = 14,
                    Metric = SimilarityMetric.DotProduct,
                },
                DefaultId = new DefaultIdOptions
                {
                    Type = DefaultIdType.ObjectId
                }
            };
            await fixture.Database.CreateCollectionAsync(collectionName, options);
            var collections = await fixture.Database.ListCollectionsAsync();
            var collectionMetadata = collections.FirstOrDefault(c => c.Name == collectionName);
            Assert.NotNull(collectionMetadata);
            Assert.Equal(14, collectionMetadata.Options.Vector.Dimension);
            Assert.Equal(SimilarityMetric.DotProduct, collectionMetadata.Options.Vector.Metric);
            Assert.Contains("metadata", collectionMetadata.Options.Indexing.Allow);
            Assert.Equal(DefaultIdType.ObjectId, collectionMetadata.Options.DefaultId.Type);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

}
