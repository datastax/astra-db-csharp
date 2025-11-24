using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Database")]
[Trait("Category", "Examples")]
public class ExamplesTests
{
    DatabaseFixture fixture;

    public ExamplesTests(AssemblyFixture assemblyFixture, DatabaseFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task BookTests()
    {
        try
        {
            var options = new CollectionDefinition
            {
                Vector = new VectorOptions
                {
                    Dimension = 5
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync("bookTests", options);

            // Insert a document into the collection
            var embeddings = new double[] { -0.05, 0.24, 0.07, 0.68, -0.34 };
            var document = new Document
            {
                { "$vector", embeddings },
                { "title", "Ocean Depths" },
                { "number_of_pages", 434 },
                { "isCheckedOut", false }
            };
            var insertResult = await collection.InsertOneAsync(document);

            var builder = Builders<Document>.Filter;
            var filter = builder.And(builder.Eq("isCheckedOut", false), builder.Eq("number_of_pages", 434));

            var findResult = await collection.FindOneAsync(filter);

            Assert.Equal("Ocean Depths", findResult["title"]);

            var bookCollection = fixture.Database.GetCollection<Book>("bookTests");
            var bookBuilder = Builders<Book>.Filter;
            var bookFilter = bookBuilder.And(bookBuilder.Eq(b => b.IsCheckedOut, false), bookBuilder.Eq(b => b.NumberOfPages, 434));
            var bookFindResult = await bookCollection.FindOneAsync(bookFilter);
            Assert.Equal("Ocean Depths", bookFindResult.Title);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync("bookTests");
        }
    }
}