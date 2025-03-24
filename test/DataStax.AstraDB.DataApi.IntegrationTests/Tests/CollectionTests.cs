using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using MongoDB.Bson;
using UUIDNext;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("DatabaseAndCollections")]
public class CollectionTests
{
    CollectionsFixture fixture;

    public CollectionTests(CollectionsFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task InsertDocumentAsync()
    {
        var collectionName = "restaurants";
        try
        {
            Console.WriteLine("Inserting a document...");
            Restaurant newRestaurant = new()
            {
                Name = "Mongo's Pizza",
                RestaurantId = "12345",
                Cuisine = "Pizza",
                Address = new()
                {
                    Street = "Pizza St",
                    ZipCode = "10003"
                },
                Borough = "Manhattan",
            };
            var collection = await fixture.Database.CreateCollectionAsync<Restaurant>(collectionName);
            var result = await collection.InsertOneAsync(newRestaurant);
            var newId = result.InsertedId;
            Assert.NotNull(newId);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task InsertDocument_UsingDefaultId()
    {
        var collectionName = "defaultId";
        try
        {
            Console.WriteLine("Inserting a document...");
            var options = new CollectionDefinition
            {
                DefaultId = new DefaultIdOptions
                {
                    Type = DefaultIdType.ObjectId
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithObjectId>(collectionName, options);
            var newItem = new SimpleObjectWithObjectId
            {
                Name = "Test Object",
            };
            var result = await collection.InsertOneAsync(newItem);
            var newId = result.InsertedId;
            var parsed = ObjectId.TryParse(newId.ToString(), out var newIdAsObjectId);
            Assert.True(parsed);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task InsertDocumentsNotOrderedAsync()
    {
        var collectionName = "simpleObjects";
        try
        {
            List<SimpleObject> items = new List<SimpleObject>();
            for (var i = 0; i < 10; i++)
            {
                items.Add(new SimpleObject()
                {
                    _id = i,
                    Name = $"Test Object {i}"
                });
            }
            ;
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject>(collectionName);
            var result = await collection.InsertManyAsync(items);
            await fixture.Database.DropCollectionAsync(collectionName);
            Assert.Equal(items.Count, result.InsertedIds.Count);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task InsertDocumentsIdsInOrder()
    {
        var collectionName = "differentIdsObjects";

        try
        {
            var date = DateTime.Now;
            var uuid4 = Uuid.NewRandom();
            Guid urlNamespaceId = Guid.Parse("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
            var uuid5 = Uuid.NewNameBased(urlNamespaceId, "https://github.com/uuid6/uuid6-ietf-draft");
            var uuid7 = Uuid.NewDatabaseFriendly(UUIDNext.Database.PostgreSql);
            var uuid8 = Uuid.NewDatabaseFriendly(UUIDNext.Database.SqlServer);
            var objectId = new ObjectId();

            List<DifferentIdsObject> items = new List<DifferentIdsObject>
            {
                new DifferentIdsObject()
                {
                    TheId = 1,
                    Name = $"Test Object Int"
                },
                new DifferentIdsObject()
                {
                    TheId = objectId,
                    Name = $"Test Object ObjectId"
                },
                new DifferentIdsObject()
                {
                    TheId = uuid4,
                    Name = $"Test Object UUID4"
                },
                new DifferentIdsObject()
                {
                    TheId = uuid5,
                    Name = $"Test Object UUID5"
                },
                new DifferentIdsObject()
                {
                    TheId = uuid7,
                    Name = $"Test Object UUID7"
                },
                new DifferentIdsObject()
                {
                    TheId = uuid8,
                    Name = $"Test Object UUID8"
                },
                new DifferentIdsObject()
                {
                    TheId = "This is an id string",
                    Name = $"Test Object String"
                }
            };

            var collection = await fixture.Database.CreateCollectionAsync<DifferentIdsObject>(collectionName);
            var result = await collection.InsertManyAsync(items, new InsertManyOptions() { InsertInOrder = true });

            Assert.Equal(items.Count, result.InsertedIds.Count);
            for (var i = 0; i < result.InsertedIds.Count; i++)
            {
                Assert.Equal((dynamic)items[i].TheId, (dynamic)result.InsertedIds[i]);
            }
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task InsertDocumentsOrderedAsync()
    {
        var collectionName = "simpleObjects";
        try
        {
            List<SimpleObject> items = new List<SimpleObject>();
            for (var i = 0; i < 10; i++)
            {
                items.Add(new SimpleObject()
                {
                    _id = i,
                    Name = $"Test Object {i}"
                });
            }
            ;
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject, int>(collectionName);
            var result = await collection.InsertManyAsync(items, new InsertManyOptions() { InsertInOrder = true });
            await fixture.Database.DropCollectionAsync(collectionName);
            Assert.Equal(items.Count, result.InsertedIds.Count);
            for (var i = 0; i < 10; i++)
            {
                var id = result.InsertedIds[i];
                Assert.Equal(i, id);
            }
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task CountDocuments_NoFilter_ReturnsCorrectCount()
    {
        var collection = fixture.SearchCollection;
        var count = await collection.CountDocumentsAsync();
        Assert.Equal(33, count.Count);
    }

    [Fact]
    public async Task CountDocuments_Filter_ReturnsCorrectCount()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq("Properties.PropertyOne", "grouptwo");
        var count = await collection.CountDocumentsAsync(filter);
        Assert.Equal(3, count.Count);
    }

}

