using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Database")]
public class DatabaseTests
{
    DatabaseFixture fixture;

    public DatabaseTests(DatabaseFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task KeyspaceTests()
    {
        var collectionName = "simpleTestKeyspaceCollection";
        var keyspaceName = "simpleTestKeyspace";
        var databaseWithKeyspace = fixture.Client.GetDatabase(fixture.DatabaseUrl);
        databaseWithKeyspace.UseKeyspace(keyspaceName);
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
    public async Task Create_And_Drop_Keyspace_DoesNotWaitForCompletion()
    {
        var keyspaceName = "dropAndDoNotWaitKeyspace";
        var database = fixture.Database;
        var admin = database.GetAdmin();

        try
        {
            await admin.CreateKeyspaceAsync(keyspaceName, true, false);
            var keyspaceExists = await admin.DoesKeyspaceExistAsync(keyspaceName);
            Assert.False(keyspaceExists, $"Keyspace '{keyspaceName}' should still be being created.");

            var maxAttempts = 30;
            while (!keyspaceExists && maxAttempts > 0)
            {
                maxAttempts--;
                // Wait for the keyspace to be created
                await Task.Delay(1000);
                keyspaceExists = await admin.DoesKeyspaceExistAsync(keyspaceName);
            }
            Assert.True(keyspaceExists, $"Keyspace '{keyspaceName}' should exist now.");

            await admin.DropKeyspaceAsync(keyspaceName, false);
            keyspaceExists = await admin.DoesKeyspaceExistAsync(keyspaceName);
            Assert.True(keyspaceExists, $"Keyspace '{keyspaceName}' should still be being dropped.");
        }
        catch (Exception ex)
        {
            await admin.DropKeyspaceAsync(keyspaceName, true);
        }
    }

    [Fact]
    public async Task Create_And_Drop_Keyspace_WaitsForCompletionWhenRequested()
    {
        var keyspaceName = "dropAndWaitKeyspace";
        var database = fixture.Database;
        var admin = database.GetAdmin();

        await admin.CreateKeyspaceAsync(keyspaceName, true, true);
        var keyspaceExists = await admin.DoesKeyspaceExistAsync(keyspaceName);
        Assert.True(keyspaceExists, $"Keyspace '{keyspaceName}' should exist after creation.");
        await admin.DropKeyspaceAsync(keyspaceName, true);
        keyspaceExists = await admin.DoesKeyspaceExistAsync(keyspaceName);
        Assert.False(keyspaceExists, $"Keyspace '{keyspaceName}' should not exist after drop.");
    }

    [Fact]
    public async Task PassTokenToDatabase()
    {
        var collectionName = "tokenToDbCollectionTest";
        var database = fixture.ClientWithoutToken.GetDatabase(fixture.DatabaseUrl, fixture.Token);
        var collection = await database.CreateCollectionAsync(collectionName);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await database.DropCollectionAsync(collectionName);
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
                Dimension = 1024,
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
                Dimension = 1024,
                Metric = SimilarityMetric.DotProduct,
                Service = new VectorServiceOptions()
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
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
                Dimension = 1024,
                Metric = SimilarityMetric.DotProduct,
                Service = new VectorServiceOptions()
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }
            }
        };
        var collection = await fixture.Database.CreateCollectionAsync(collectionName, options);
        Assert.NotNull(collection);
        Assert.Equal(collectionName, collection.CollectionName);
        await fixture.Database.DropCollectionAsync(collectionName);
    }

    [Fact]
    public async Task CreateCollection_ForHybridSearch()
    {
        var collectionName = "collectionHybridSearch";
        var options = new CollectionDefinition
        {
            Vector = new VectorOptions
            {
                Dimension = 1024,
                Metric = SimilarityMetric.DotProduct,
                Service = new VectorServiceOptions()
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }
            },
            Lexical = new LexicalOptions
            {
                Analyzer = new AnalyzerOptions
                {
                    Tokenizer = new TokenizerOptions
                    {
                        Name = "standard",
                        Arguments = new Dictionary<string, object>
                        {
                            { "name", "standard" }
                        }
                    },
                    Filters = new List<string>
                    {
                        "lowercase",
                        "stop",
                        "porterstem",
                        "asciifolding"
                    },
                    CharacterFilters = new List<string>()
                },
                Enabled = true
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

    [Fact]
    public async Task CreateTable_Default()
    {
        try
        {
            var table = await fixture.Database.CreateTableAsync<SimpleRowObject>();
            Assert.NotNull(table);
            var definitions = await fixture.Database.ListTablesAsync();
            var definition = definitions.FirstOrDefault(d => d.Name == typeof(SimpleRowObject).Name);
            Assert.NotNull(definition);
            Assert.Single(definition.TableDefinition.PrimaryKey.Keys);
            Assert.Equal("Name", definition.TableDefinition.PrimaryKey.Keys[0]);
        }
        finally
        {
            await fixture.Database.DropTableAsync<SimpleRowObject>();
        }
    }

    //TODO re-enable this test once the api issue is fixed (https://github.com/stargate/data-api/issues/2141)
    // [Fact]
    // public async Task CreateTable_DataTypesTest_FromObject()
    // {
    //     const string tableName = "tableDataTypesTestFromObject";
    //     try
    //     {
    //         var table = await fixture.Database.CreateTableAsync<RowTestObject>(tableName);
    //         Assert.NotNull(table);
    //         var definitions = await fixture.Database.ListTablesAsync();
    //         var definition = definitions.FirstOrDefault(d => d.Name == tableName);
    //         Assert.NotNull(definition);
    //         Assert.Equal(4, (definition.TableDefinition.Columns["Vector"] as VectorColumn).Dimension);

    //         var row = new RowTestObject
    //         {
    //             Name = "Test",
    //             Vector = new float[4] { 1, 2, 3, 4 },
    //             StringToVectorize = "TestStringToVectorize",
    //             Text = "TestText",
    //             Inet = IPAddress.Parse("192.168.0.1"),
    //             Int = int.MaxValue,
    //             TinyInt = byte.MaxValue,
    //             SmallInt = short.MaxValue,
    //             BigInt = long.MaxValue,
    //             Decimal = decimal.MaxValue,
    //             Double = double.MaxValue,
    //             Float = float.MaxValue,
    //             IntDictionary = new Dictionary<string, int>() { { "One", 1 }, { "Two", 2 } },
    //             DecimalDictionary = new Dictionary<string, decimal>() { { "One", 1.11111m }, { "Two", 2.22222m } },
    //             StringSet = new HashSet<string>() { "HashSetOne", "HashSetTwo" },
    //             IntSet = new HashSet<int>() { 1, 2 },
    //             StringList = new List<string>() { "One", "Two" },
    //             ObjectList = new List<Properties>() {
    //                 new Properties() { PropertyOne = "OneOne", PropertyTwo = "OneTwo" },
    //                 new Properties() { PropertyOne = "TwoOne", PropertyTwo = "TwoTwo" } },
    //             Boolean = false,
    //             Date = DateTime.Now,
    //             UUID = Guid.NewGuid(),
    //             Blob = Encoding.ASCII.GetBytes("Test Blob"),
    //             Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us1ns")
    //         };
    //         var result = await table.InsertManyAsync(new List<RowTestObject> { row });
    //         Assert.Equal(1, result.InsertedCount);
    //         var resultSet = table.Find();
    //         var resultList = resultSet.ToList();
    //         var resultRow = resultList.First();
    //         Assert.Equal(row.Name, resultRow.Name);
    //         Assert.Equal(row.Vector, resultRow.Vector);
    //         Assert.Equal(row.Text, resultRow.Text);
    //         Assert.Equal(row.Inet, resultRow.Inet);
    //         Assert.Equal(row.Int, resultRow.Int);
    //         Assert.Equal(row.TinyInt, resultRow.TinyInt);
    //         Assert.Equal(row.SmallInt, resultRow.SmallInt);
    //         Assert.Equal(row.BigInt, resultRow.BigInt);
    //         Assert.Equal(row.Decimal, resultRow.Decimal);
    //         Assert.Equal(row.Double, resultRow.Double);
    //         Assert.Equal(row.Float, resultRow.Float);
    //         Assert.Equal(row.IntDictionary, resultRow.IntDictionary);
    //         Assert.Equal(row.DecimalDictionary, resultRow.DecimalDictionary);
    //         Assert.Equal(row.StringSet, resultRow.StringSet);
    //         Assert.Equal(row.IntSet, resultRow.IntSet);
    //         Assert.Equal(row.StringList, resultRow.StringList);
    //         Assert.Equal(JsonSerializer.Serialize(row.ObjectList), JsonSerializer.Serialize(resultRow.ObjectList));
    //         Assert.Equal(row.Boolean, resultRow.Boolean);
    //         Assert.Equal(row.Date.ToUniversalTime().ToString("MMddyyhhmmss"), resultRow.Date.ToUniversalTime().ToString("MMddyyhhmmss"));
    //         Assert.Equal(row.UUID, resultRow.UUID);
    //         Assert.Equal(row.Blob, resultRow.Blob);
    //         Assert.Equal(row.Duration, resultRow.Duration);

    //     }
    //     finally
    //     {
    //         await fixture.Database.DropTableAsync(tableName);
    //     }
    // }

    [Fact]
    public async Task CreateTable_DataTypesTest_FromDefinition()
    {
        var tableName = "testDataTypesFromDefinition";
        try
        {
            var createDefinition = new TableDefinition()
                .AddTextColumn("Name")
                .AddVectorColumn("Vector", 1024)
                .AddVectorizeColumn("StringToVectorize", 1024, new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                })
                .AddPrimaryKey("Name");

            var table = await fixture.Database.CreateTableAsync(tableName, createDefinition);
            Assert.NotNull(table);
            var definitions = await fixture.Database.ListTablesAsync();
            var definition = definitions.FirstOrDefault(d => d.Name == tableName);
            Assert.NotNull(definition);
            Assert.Equal(1024, (definition.TableDefinition.Columns["Vector"] as VectorColumn).Dimension);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task CreateTable_CompositePrimaryKey_FromObject()
    {
        try
        {
            var table = await fixture.Database.CreateTableAsync<CompositePrimaryKey>();
            Assert.NotNull(table);
            var definitions = await fixture.Database.ListTablesAsync();
            var definition = definitions.FirstOrDefault(d => d.Name == typeof(CompositePrimaryKey).Name);
            Assert.NotNull(definition);
            Assert.Equal(2, definition.TableDefinition.PrimaryKey.Keys.Count);
            Assert.Equal("KeyOne", definition.TableDefinition.PrimaryKey.Keys[0]);
            Assert.Equal("KeyTwo", definition.TableDefinition.PrimaryKey.Keys[1]);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
        finally
        {
            await fixture.Database.DropTableAsync<CompositePrimaryKey>();
        }
    }

    [Fact]
    public async Task CreateTable_CompoundPrimaryKey_FromObject()
    {
        try
        {
            var table = await fixture.Database.CreateTableAsync<CompoundPrimaryKey>();
            Assert.NotNull(table);
            var definitions = await fixture.Database.ListTablesAsync();
            var definition = definitions.FirstOrDefault(d => d.Name == typeof(CompoundPrimaryKey).Name);
            Assert.NotNull(definition);
            Assert.Equal(2, definition.TableDefinition.PrimaryKey.Keys.Count);
            Assert.Equal("KeyOne", definition.TableDefinition.PrimaryKey.Keys[0]);
            Assert.Equal("KeyTwo", definition.TableDefinition.PrimaryKey.Keys[1]);
            Assert.Equal(2, definition.TableDefinition.PrimaryKey.Sorts.Count);
            Assert.Equal(SortDirection.Ascending, definition.TableDefinition.PrimaryKey.Sorts["SortOneAscending"]);
            Assert.Equal(SortDirection.Descending, definition.TableDefinition.PrimaryKey.Sorts["SortTwoDescending"]);
        }
        finally
        {
            await fixture.Database.DropTableAsync<CompoundPrimaryKey>();
        }
    }

    [Fact]
    public async Task CreateTable_CompoundPrimaryKey_FromDefinition()
    {
        var tableName = "testCompoundPrimaryKeyFromDefinition";
        try
        {
            var createDefinition = new TableDefinition()
                .AddTextColumn("KeyOne")
                .AddTextColumn("KeyTwo")
                .AddTextColumn("Name")
                .AddTextColumn("SortOneAscending")
                .AddTextColumn("SortTwoDescending")
                .AddCompoundPrimaryKey("KeyOne", 1)
                .AddCompoundPrimaryKey("KeyTwo", 2)
                .AddCompoundPrimaryKeySort("SortOneAscending", 1, SortDirection.Ascending)
                .AddCompoundPrimaryKeySort("SortTwoDescending", 2, SortDirection.Descending);

            var table = await fixture.Database.CreateTableAsync(tableName, createDefinition);
            Assert.NotNull(table);
            const int retries = 6;
            const int waitInSeconds = 5;
            int tryNumber = 0;

            TableInfo definition = null;

            while (tryNumber < retries && definition == null)
            {
                var definitions = await fixture.Database.ListTablesAsync();
                definition = definitions.FirstOrDefault(d => d.Name == tableName);
                if (definition != null)
                {
                    return;
                }
                await Task.Delay(waitInSeconds * 1000);
                tryNumber++;
            }

            Assert.NotNull(definition);
            Assert.Equal(2, definition.TableDefinition.PrimaryKey.Keys.Count);
            Assert.Equal("KeyOne", definition.TableDefinition.PrimaryKey.Keys[0]);
            Assert.Equal("KeyTwo", definition.TableDefinition.PrimaryKey.Keys[1]);
            Assert.Equal(2, definition.TableDefinition.PrimaryKey.Sorts.Count);
            Assert.Equal(SortDirection.Ascending, definition.TableDefinition.PrimaryKey.Sorts["SortOneAscending"]);
            Assert.Equal(SortDirection.Descending, definition.TableDefinition.PrimaryKey.Sorts["SortTwoDescending"]);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
        try
        {
            var table = await fixture.Database.CreateTableAsync<CompoundPrimaryKey>();
            Assert.NotNull(table);
            var definitions = await fixture.Database.ListTablesAsync();
            var definition = definitions.FirstOrDefault(d => d.Name == typeof(CompoundPrimaryKey).Name);
            Assert.NotNull(definition);
            Assert.Equal(2, definition.TableDefinition.PrimaryKey.Keys.Count);
            Assert.Equal("KeyOne", definition.TableDefinition.PrimaryKey.Keys[0]);
            Assert.Equal("KeyTwo", definition.TableDefinition.PrimaryKey.Keys[1]);
            Assert.Equal(2, definition.TableDefinition.PrimaryKey.Sorts.Count);
            Assert.Equal(SortDirection.Ascending, definition.TableDefinition.PrimaryKey.Sorts["SortOneAscending"]);
            Assert.Equal(SortDirection.Descending, definition.TableDefinition.PrimaryKey.Sorts["SortTwoDescending"]);
        }
        finally
        {
            await fixture.Database.DropTableAsync<CompoundPrimaryKey>();
        }
    }

    [Fact]
    public async Task CreateTableFromObject_InvalidPrimaryKeys_ThrowsException()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await fixture.Database.CreateTableAsync<BrokenCompositePrimaryKey>());
    }

    [Fact]
    public async Task CreateTableFromObject_InvalidPrimaryKeySorts_ThrowsException()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await fixture.Database.CreateTableAsync<BrokenCompoundPrimaryKey>());
    }

    [Fact]
    public async Task GetTable()
    {
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowBook>();
            Assert.NotNull(table);
            var foundTable = fixture.Database.GetTable<RowBook>();
            Assert.NotNull(foundTable);
            var foundTable2 = fixture.Database.GetTable<RowBook>("bookTestTable");
            Assert.NotNull(foundTable2);
            //add a row to the first table, then make sure it exists in the second (both should be the same underlying table)
            var row = new RowBook()
            {
                Title = "Desert Peace",
                Author = "Walter Dray",
                NumberOfPages = 355,
                DueDate = DateTime.Now - TimeSpan.FromDays(2),
                Genres = new HashSet<string> { "Fiction" }
            };
            var result = await table.InsertOneAsync(row);
            Assert.Equal(1, result.InsertedCount);
            var id = result.InsertedIds.First().First().ToString();
            var filter = Builders<RowBook>.Filter.Eq(b => b.Title, id);
            var foundRow = await foundTable2.FindOneAsync(filter);
            Assert.Equal(row.Title, foundRow.Title);
        }
        finally
        {
            await fixture.Database.DropTableAsync<RowBook>();
        }
    }

    [Fact]
    public async Task DropNonExistentTable_ThrowsException()
    {
        await Assert.ThrowsAsync<CommandException>(async () => await fixture.Database.DropTableAsync("nonExistentTable"));
    }

    [Fact]
    public async Task DropNonExistentTable_DoesNotThrowException_WhenNotExistsIsTrue()
    {
        await fixture.Database.DropTableAsync("nonExistentTable", true);
    }

    [Fact]
    public async Task DropExistingTable()
    {
        var tableName = "testDropExistingTable";
        var table = await fixture.Database.CreateTableAsync<RowBook>(tableName);
        Assert.NotNull(table);
        await fixture.Database.DropTableAsync(tableName);
    }

}
