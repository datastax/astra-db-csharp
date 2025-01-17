using DataStax.AstraDB.DataAPI;
using DataStax.AstraDB.DataAPI.Core;
using DataStax.AstraDB.DataAPI.Collections;

namespace DataStax.AstraDB.DataAPI.IntegrationTests.Tests;

public class CollectionTests
{
    public static async Task InsertDocumentAsync(Database database)
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
            await database.CreateCollectionAsync("restaurants");
            var collection = database.GetCollection<Restaurant>("restaurants");
            var result = await collection.InsertOneAsync(newRestaurant);
            var newId = result.InsertedId;
            Console.WriteLine("Insertion Succeeded: " + newId);
        }
        catch (Exception e)
        {
            //TODO switch to test framework and fail test
            Console.WriteLine("Unable to insert due to an error: " + e);
        }
    }

}

public class Restaurant
{
    public Guid Id { get; set; }
    public required string Name { get; set; }
    public required string RestaurantId { get; set; }
    public required string Cuisine { get; set; }
    public required Address Address { get; set; }
    public required string Borough { get; set; }
    public List<GradeEntry>? Grades { get; set; }
}

public class Address
{
    public string? Building { get; set; }
    public double[]? Coordinates { get; set; }
    public required string Street { get; set; }
    public required string ZipCode { get; set; }
}

public class GradeEntry
{
    public DateTime Date { get; set; }
    public string? Grade { get; set; }
    public float? Score { get; set; }
}