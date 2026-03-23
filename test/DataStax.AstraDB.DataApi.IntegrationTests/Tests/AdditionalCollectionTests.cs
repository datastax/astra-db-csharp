using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using System.Text.Json;
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
                    TimestampWithKind = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Local),
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
                    TimestampWithKind = DateTime.SpecifyKind(DateTime.Now.AddDays(i), DateTimeKind.Local),
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
    public async Task Test_DoubleAndFloatConverters_Typed()
    {
        var collectionName = "collectionTestDoubleFloatConverters";
        try
        {
            var typed = await fixture.Database.CreateCollectionAsync<DoubleFloatTypeTest>(collectionName);
            
            var typedDocs = new List<DoubleFloatTypeTest>
            {
                new() { DoubleValue = 123.456, FloatValue = 78.9f, FloatDoubleMap = new Dictionary<float, double> { { 1.1f, 2.2 } }, FloatList = new List<float> { 3.3f, 4.4f } },
                new() { DoubleValue = double.NaN, FloatValue = float.NaN, FloatDoubleMap = new Dictionary<float, double> { { float.NaN, double.NaN } }, FloatList = new List<float> { float.NaN } },
                new() { DoubleValue = double.PositiveInfinity, FloatValue = null, FloatDoubleMap = new Dictionary<float, double> { { float.PositiveInfinity, double.NegativeInfinity } }, FloatList = new List<float> { float.PositiveInfinity } },
                new() { DoubleValue = double.NegativeInfinity, FloatValue = float.NegativeInfinity, FloatDoubleMap = new Dictionary<float, double> { { 0.0f, 0.0 } }, FloatList = new List<float> { 0.0f } }
            };
            
            var result = await typed.InsertManyAsync(typedDocs);
            Assert.Equal(4, result.InsertedCount);
            
            var row0 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.Filter.Eq(x => x.DoubleValue, 123.456));
            Assert.Equal(123.456, row0.DoubleValue.Value, 5);
            Assert.Equal(78.9f, row0.FloatValue.Value, 5);
            Assert.Equal(new Dictionary<float, double> { { 1.1f, 2.2 } }, row0.FloatDoubleMap);
            Assert.Equal(new List<float> { 3.3f, 4.4f }, row0.FloatList);
            
            var row1 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.Filter.Eq(x => x.DoubleValue, double.NaN));
            Assert.True(double.IsNaN(row1.DoubleValue.Value));
            Assert.True(float.IsNaN(row1.FloatValue.Value));
            Assert.Equal(new Dictionary<float, double> { { float.NaN, double.NaN } }, row1.FloatDoubleMap);
            Assert.Equal(new List<float> { float.NaN }, row1.FloatList);
            
            var row2 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.Filter.Eq(x => x.DoubleValue, double.PositiveInfinity));
            Assert.True(double.IsPositiveInfinity(row2.DoubleValue.Value));
            Assert.Null(row2.FloatValue);
            Assert.Equal(new Dictionary<float, double> { { float.PositiveInfinity, double.NegativeInfinity } }, row2.FloatDoubleMap);
            Assert.Equal(new List<float> { float.PositiveInfinity }, row2.FloatList);
            
            var row3 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.Filter.Eq(x => x.DoubleValue, double.NegativeInfinity));
            Assert.True(double.IsNegativeInfinity(row3.DoubleValue.Value));
            Assert.True(float.IsNegativeInfinity(row3.FloatValue.Value));
            Assert.Equal(new Dictionary<float, double> { { 0.0f, 0.0 } }, row3.FloatDoubleMap);
            Assert.Equal(new List<float> { 0.0f }, row3.FloatList);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task Test_DoubleAndFloatConverters_Untyped()
    {
        var collectionName = "collectionTestDoubleFloatConverters";
        try
        {
            var untyped = await fixture.Database.CreateCollectionAsync<Document>(collectionName);
            
            var badUntypedDocs = new List<Document>
            {
                new() { ["DoubleValue"] = double.PositiveInfinity, ["FloatValue"] = float.PositiveInfinity },
                new() { ["DoubleValue"] = double.NegativeInfinity, ["FloatValue"] = float.NegativeInfinity },
                new() { ["DoubleValue"] = double.NaN, ["FloatValue"] = float.NaN },
            };
            
            foreach (var doc in badUntypedDocs)
            {
                await Assert.ThrowsAsync<ArgumentException>(async () =>
                {
                    await untyped.InsertOneAsync(doc);
                });
            }
            
            var okUntypedDocs = new List<Document>
            {
                new() { ["DoubleValue"] = "+Infinity", ["FloatValue"] = "+Infinity" },
                new() { ["DoubleValue"] = "-Infinity", ["FloatValue"] = "-NegativeInfinity" },
                new() { ["DoubleValue"] = "NaN", ["FloatValue"] = "NaN" },
            };
            
            var insertResult = await untyped.InsertManyAsync(okUntypedDocs);
            Assert.Equal(okUntypedDocs.Count, insertResult.InsertedIds.Count);

            foreach (var doc in okUntypedDocs) // so stupid there's no easy way to compare sets of dictionaries
            {
                var found = await untyped.FindOneAsync(Builders<Document>.Filter.Eq("DoubleValue", doc["DoubleValue"]));
                Assert.NotNull(found);
                Assert.Equal(doc["DoubleValue"], found["DoubleValue"]);
                Assert.Equal(doc["FloatValue"], found["FloatValue"]);
            }
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }
}
