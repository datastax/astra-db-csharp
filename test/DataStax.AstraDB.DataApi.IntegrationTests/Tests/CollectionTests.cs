using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.SerDes;
using MongoDB.Bson;
using System.Text.Json.Serialization;
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
    public async Task DefaultId_ObjectId()
    {
        var collectionName = "defaultIdObjectId";
        try
        {
            var collectionDefinition = new CollectionDefinition()
            {
                DefaultId = new DefaultIdOptions()
                {
                    Type = DefaultIdType.ObjectId
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithObjectId, ObjectId>(collectionName, collectionDefinition);
            var newObject = new SimpleObjectWithObjectId()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            var secondObject = new SimpleObjectWithObjectId()
            {
                Name = "Test Object 2",
            };

            result = await collection.InsertOneAsync(secondObject);
            var newId2 = result.InsertedId;

            Assert.NotEqual(newId, newId2);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefaultId_UUID4()
    {
        var collectionName = "defaultIdUUID4";
        try
        {
            var collectionDefinition = new CollectionDefinition()
            {
                DefaultId = new DefaultIdOptions()
                {
                    Type = DefaultIdType.Uuid
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithGuidId, Guid>(collectionName, collectionDefinition);
            var newObject = new SimpleObjectWithGuidId()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            var secondObject = new SimpleObjectWithGuidId()
            {
                Name = "Test Object 2",
            };

            result = await collection.InsertOneAsync(secondObject);
            var newId2 = result.InsertedId;

            Assert.NotEqual(newId, newId2);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefaultId_UUIDV6()
    {
        var collectionName = "defaultIdUUIDV6";
        try
        {
            var collectionDefinition = new CollectionDefinition()
            {
                DefaultId = new DefaultIdOptions()
                {
                    Type = DefaultIdType.UuidV6
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithGuidId, Guid>(collectionName, collectionDefinition);
            var newObject = new SimpleObjectWithGuidId()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            var secondObject = new SimpleObjectWithGuidId()
            {
                Name = "Test Object 2",
            };

            result = await collection.InsertOneAsync(secondObject);
            var newId2 = result.InsertedId;

            Assert.NotEqual(newId, newId2);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefaultId_UUIDV7()
    {
        var collectionName = "defaultIdUUIDV7";
        try
        {
            var collectionDefinition = new CollectionDefinition()
            {
                DefaultId = new DefaultIdOptions()
                {
                    Type = DefaultIdType.UuidV7
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithGuidId, Guid>(collectionName, collectionDefinition);
            var newObject = new SimpleObjectWithGuidId()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            var secondObject = new SimpleObjectWithGuidId()
            {
                Name = "Test Object 2",
            };

            result = await collection.InsertOneAsync(secondObject);
            var newId2 = result.InsertedId;

            Assert.NotEqual(newId, newId2);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    public class DefaultIdUUID4
    {
        [DocumentId]
        public Guid? Id { get; set; }
        public string Name { get; set; }
    }

    public class DefaultIdUUIDV6
    {
        [DocumentId(DefaultIdType.UuidV6)]
        public Guid? Id { get; set; }
        public string Name { get; set; }
    }

    public class DefaultIdUUIDV7
    {
        [DocumentId(DefaultIdType.UuidV7)]
        public Guid? Id { get; set; }
        public string Name { get; set; }
    }

    public class DefaultIdUUIDObjectId
    {
        [DocumentId(DefaultIdType.ObjectId)]
        public ObjectId? Id { get; set; }
        public string Name { get; set; }
    }

    [Fact]
    public async Task DefaultId_UUIDV4_FromObject()
    {
        var collectionName = "defaultIdUUIDV4FromObject";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<DefaultIdUUID4, Guid>(collectionName);
            var newObject = new DefaultIdUUID4()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            Assert.NotEqual(Guid.Empty, newId);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefaultId_UUIDV6_FromObject()
    {
        var collectionName = "defaultIdUUIDV6FromObject";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<DefaultIdUUIDV6, Guid>(collectionName);
            var newObject = new DefaultIdUUIDV6()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            Assert.NotEqual(Guid.Empty, newId);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefaultId_UUIDV7_FromObject()
    {
        var collectionName = "defaultIdUUIDV7FromObject";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<DefaultIdUUIDV7, Guid>(collectionName);
            var newObject = new DefaultIdUUIDV7()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            Assert.NotEqual(Guid.Empty, newId);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefaultId_UUIDObjectId_FromObject()
    {
        var collectionName = "defaultIdUUIDObjectIdFromObject";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<DefaultIdUUIDObjectId, ObjectId>(collectionName);
            var newObject = new DefaultIdUUIDObjectId()
            {
                Name = "Test Object 1",
            };

            var result = await collection.InsertOneAsync(newObject);
            var newId = result.InsertedId;

            Assert.NotEqual(ObjectId.Empty, newId);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefaultId_WrongTypes()
    {
        var collectionName = "defaultIdWrongTypes";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject>(collectionName);
            var newObject = new SimpleObject()
            {
                Name = "Test Object 1",
            };

            await Assert.ThrowsAnyAsync<Exception>(async () => await collection.InsertOneAsync(newObject));
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefiningIndexing_Allow()
    {
        var collectionName = "definingIndexingAllow";
        try
        {
            var collectionDefinition = new CollectionDefinition()
            {
                Indexing = new IndexingOptions()
                {
                    Allow = new List<string>()
                    {
                        "Name"
                    }
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject>(collectionName, collectionDefinition);

            await collection.InsertManyAsync(new List<SimpleObject>()
            {
                new()
                {
                    _id = 1,
                    Name = "Name1",
                    Properties = new Properties() {
                        PropertyOne = "groupOne1",
                        PropertyTwo = "propertyTwo1"
                    }
                },
                new()
                {
                    _id = 2,
                    Name = "Name2",
                    Properties = new Properties() {
                        PropertyOne = "groupOne2",
                        PropertyTwo = "propertyTwo2"
                    }
                },
                new()
                {
                    _id = 3,
                    Name = "Name3",
                    Properties = new Properties() {
                        PropertyOne = "groupOne3",
                        PropertyTwo = "propertyTwo3"
                    }
                },
            });

            var result = collection.Find(Builders<SimpleObject>.Filter.Eq(s => s.Name, "Name1")).ToList();
            Assert.Single(result);
            //Cannot filter by a non-indexed column
            var actInvalid = () => collection.Find(Builders<SimpleObject>.Filter.Eq(s => s.Properties.PropertyOne, "groupOne1")).ToList();
            Assert.Throws<CommandException>(actInvalid);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task DefiningIndexing_Deny()
    {
        var collectionName = "definingIndexingDeny";
        try
        {
            var collectionDefinition = new CollectionDefinition()
            {
                Indexing = new IndexingOptions()
                {
                    Deny = new List<string>()
                    {
                        "Name"
                    }
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject>(collectionName, collectionDefinition);

            await collection.InsertManyAsync(new List<SimpleObject>()
            {
                new()
                {
                    _id = 1,
                    Name = "Name1",
                    Properties = new Properties() {
                        PropertyOne = "groupOne1",
                        PropertyTwo = "propertyTwo1"
                    }
                },
                new()
                {
                    _id = 2,
                    Name = "Name2",
                    Properties = new Properties() {
                        PropertyOne = "groupOne2",
                        PropertyTwo = "propertyTwo2"
                    }
                },
                new()
                {
                    _id = 3,
                    Name = "Name3",
                    Properties = new Properties() {
                        PropertyOne = "groupOne3",
                        PropertyTwo = "propertyTwo3"
                    }
                },
            });

            var result = collection.Find(Builders<SimpleObject>.Filter.Eq(s => s.Properties.PropertyOne, "groupOne1")).ToList();
            Assert.Single(result);
            var actInvalid = () => collection.Find(Builders<SimpleObject>.Filter.Eq(s => s.Name, "Name1")).ToList();
            Assert.Throws<CommandException>(actInvalid);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task InsertDocumentAsync()
    {
        var collectionName = "restaurants";
        try
        {
            Restaurant newRestaurant = new()
            {
                Name = "Astra's Pizza",
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
    public async Task InsertDocument_UsingDefault_ObjectId()
    {
        var collectionName = "defaultId";
        try
        {
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

