using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Collections;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Tests;

[CollectionDefinition("Collection Collection")]
public class DatabaseCollection : ICollectionFixture<ClientFixture>
{

}

[Collection("Collection Collection")]
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
            await fixture.Database.CreateCollectionAsync("restaurants");
            var collection = fixture.Database.GetCollection<Restaurant>("restaurants");
            var result = await collection.InsertOneAsync(newRestaurant);
            var newId = result.InsertedId;
            Assert.NotNull(newId);
        }
        catch (Exception e)
        {
            Assert.Fail(e.Message);
        }
    }

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