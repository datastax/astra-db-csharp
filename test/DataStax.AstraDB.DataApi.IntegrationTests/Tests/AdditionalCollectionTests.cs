using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Results;
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
    public async Task EmbeddingApiKey_Test()
    {
        var collectionName = "testEmbeddingApiKey";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<Book>(collectionName, new DatabaseCollectionCommandOptions()
            {
                EmbeddingApiKey = "test-api-key-here"
            });
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
            //Manually verify the API key is included in the request by checking the logs
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

    [Fact]
    public async Task Test_DateTimeTypes()
    {
        var collectionName = "collectionTestDateTimeTypes";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<DateTypeTest>(collectionName);

            List<DateTypeTest> documents = new List<DateTypeTest>();
            for (var i = 0; i < 5; i++)
            {
                documents.Add(new DateTypeTest()
                {
                    Id = i,
                    Timestamp = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Unspecified),
                    Date = new DateOnly(2000, 1, i + 1),
                    Time = new TimeOnly(12, i),
                    TimestampWithKind = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(i), DateTimeKind.Utc),
                });
            }
            for (var i = 5; i < 10; i++)
            {
                documents.Add(new DateTypeTest()
                {
                    Id = i,
                    Timestamp = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Unspecified),
                    Date = new DateOnly(2000, 1, i + 1),
                    Time = new TimeOnly(12, i),
                    TimestampWithKind = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(i), DateTimeKind.Utc),
                    MaybeDate = new DateOnly(2000, 1, i + 1),
                    MaybeTime = new TimeOnly(12, i),
                    MaybeTimestamp = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Unspecified),
                });
            }

            // Insert the data
            var result = await collection.InsertManyAsync(documents);

            Console.WriteLine($"Inserted {result.InsertedCount} rows");

            Assert.Equal(10, result.InsertedCount);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task TestInsertError()
    {
        var collectionName = "insertErrorTest";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObject>(collectionName);
            var row1 = new SimpleObject()
            {
                _id = 1,
                Name = "Test 1"

            };
            var row2 = new SimpleObject()
            {
                _id = 2,
                Name = "Test 2"
            };

            var insertOneResult = await collection.InsertOneAsync(row1);
            Assert.NotNull(insertOneResult.InsertedId);

            await Assert.ThrowsAsync<BulkOperationException<CollectionInsertManyResult<object>>>(async () =>
            {
                await collection.InsertManyAsync(new List<SimpleObject> { row1, row2 });
            });

        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task TestCollectionNameAttribute()
    {
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<CollectionNameObject>();
            var row1 = new CollectionNameObject()
            {
                _id = 1,
                Test = "Test 1"

            };

            var insertOneResult = await collection.InsertOneAsync(row1);
            Assert.NotNull(insertOneResult.InsertedId);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync<CollectionNameObject>();
        }
    }

    [Fact]
    public async Task DateTime_Timezone_Tests()
    {
        var collectionName = "dateTimeTestCollection";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<CollectionDatetimeObject>(collectionName);
            var insertee = new CollectionDatetimeObject {
                _id = "from_cs",
                dt_naive = new DateTime(2024, 6, 15, 10, 30, 0, 500, DateTimeKind.Unspecified),
                dt_aware = new DateTime(2024, 6, 15, 10, 30, 0, 500, DateTimeKind.Utc),
                dt_unspecified = new DateTime(2024, 6, 15, 10, 30, 0, 500, DateTimeKind.Unspecified)
            };
            await collection.InsertOneAsync(insertee);

            var filter = Builders<CollectionDatetimeObject>.CollectionFilter.Eq(d => d._id, "from_cs");
            var reread = await collection.FindOneAsync(filter);

            Assert.Equal(insertee.dt_naive, reread.dt_naive);
            Assert.Equal(insertee.dt_aware, reread.dt_aware);
            Assert.Equal(insertee.dt_unspecified, reread.dt_unspecified);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

}

[CollectionName("testCollectionNameViaAttribute")]
public class CollectionNameObject
{
    public int? _id { get; set; }
    public string Test { get; set; }
}

