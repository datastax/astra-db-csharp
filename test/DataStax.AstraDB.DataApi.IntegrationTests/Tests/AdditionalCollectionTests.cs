using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Database")]
public class AdditionalCollectionTests
{
    DatabaseFixture fixture;

    public AdditionalCollectionTests(AssemblyFixture assemblyFixture, DatabaseFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task Book_Insert_Retrieve_Tests()
    {
        var collectionName = "testBookObject";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<Book>(collectionName);
            var items = new List<Book>() {
                new Book()
                {
                    Title = "Test Book 1",
                    Author = "Test Author 1",
                    NumberOfPages = 100,
                },
                new Book()
                {
                    Title = "Test Book 2",
                    Author = "Test Author 2",
                    NumberOfPages = 200,
                },
            };
            var insertResult = await collection.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var allBooks = collection.Find().ToList();
            Assert.Equal(items.Count, allBooks.Count);
            var book = allBooks.Find(book => book.Title.Equals("Test Book 1"));
            Assert.Equal("Test Book 1", book.Title);
            Assert.Equal("Test Author 1", book.Author);
            Assert.Equal(100, book.NumberOfPages);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task Book_Projection_Tests()
    {
        var collectionName = "testBookProjection";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<Book>(collectionName);
            var items = new List<Book>() {
                new Book()
                {
                    Title = "Test Book 1",
                    Author = "Test Author 1",
                    NumberOfPages = 100,
                },
                new Book()
                {
                    Title = "Test Book 2",
                    Author = "Test Author 2",
                    NumberOfPages = 200,
                },
            };
            var insertResult = await collection.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var inclusiveProjection = Builders<Book>.Projection
                .Include(x => x.Author).Include(x => x.Title);
            var allBooks = collection.Find().Project(inclusiveProjection).ToList();
            Assert.Equal(items.Count, allBooks.Count);
            var book = allBooks.Find(book => book.Title.Equals("Test Book 1"));
            Assert.Equal("Test Book 1", book.Title);
            Assert.Equal("Test Author 1", book.Author);
            Assert.Null(book.NumberOfPages);
            //Use property names instead of expressions
            inclusiveProjection = Builders<Book>.Projection
                .Include("number_of_pages").Include("title");
            allBooks = collection.Find().Project(inclusiveProjection).ToList();
            Assert.Equal(items.Count, allBooks.Count);
            book = allBooks.Find(book => book.Title.Equals("Test Book 1"));
            Assert.Equal("Test Book 1", book.Title);
            Assert.Null(book.Author);
            Assert.Equal(100, book.NumberOfPages);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

}

