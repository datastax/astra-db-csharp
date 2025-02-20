using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Tests;

[Collection("DatabaseAndCollections")]
public class CollectionTests
{
    ClientFixture fixture;

    public CollectionTests(ClientFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task InsertDocumentAsync()
    {
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
            var collectionName = "restaurants";
            var collection = await fixture.Database.CreateCollectionAsync<Restaurant>(collectionName);
            var result = await collection.InsertOneAsync(newRestaurant);
            await fixture.Database.DropCollectionAsync(collectionName);
            var newId = result.InsertedId;
            Assert.NotNull(newId);
        }
        catch (Exception e)
        {
            Assert.Fail(e.Message);
        }
    }

    [Fact]
    public async Task InsertDocumentsNotOrderedAsync()
    {
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
            var collectionName = "simpleObjects";
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject>(collectionName);
            var result = await collection.InsertManyAsync(items);
            await fixture.Database.DropCollectionAsync(collectionName);
            Assert.Equal(items.Count, result.InsertedIds.Count);
        }
        catch (Exception e)
        {
            Assert.Fail(e.Message);
        }
    }

    [Fact]
    public async Task InsertDocumentsOrderedAsync()
    {
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
            var collectionName = "simpleObjects";
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject>(collectionName);
            var result = await collection.InsertManyAsync(items, new InsertManyOptions() { InsertInOrder = true });
            await fixture.Database.DropCollectionAsync(collectionName);
            Assert.Equal(items.Count, result.InsertedIds.Count);
            for (var i = 0; i < 10; i++)
            {
                var id = result.InsertedIds[i];
                Assert.Equal(i, id);
            }
        }
        catch (Exception e)
        {
            Assert.Fail(e.Message);
        }
    }

}

public class SimpleObject
{
    public int _id { get; set; }
    public string Name { get; set; }
}

public class Restaurant
{
    public Restaurant() { }
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string RestaurantId { get; set; }
    public string Cuisine { get; set; }
    public Address Address { get; set; }
    public string Borough { get; set; }
    public List<GradeEntry> Grades { get; set; }
}

public class Address
{
    public string Building { get; set; }
    public double[] Coordinates { get; set; }
    public string Street { get; set; }
    public string ZipCode { get; set; }
}

public class GradeEntry
{
    public DateTime Date { get; set; }
    public string Grade { get; set; }
    public float? Score { get; set; }
}