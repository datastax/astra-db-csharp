using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using System.Text;
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
    public async Task EmbeddingAPIKey_Test()
    {
        var collectionName = "testEmbeddingAPIKey";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<Book>(collectionName, new CreateCollectionOptions()
            {
                EmbeddingAPIKey = "test-api-key-here"
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

            var row0 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.CollectionFilter.Eq(x => x.DoubleValue, 123.456));
            Assert.Equal(123.456, row0.DoubleValue.Value, 5);
            Assert.Equal(78.9f, row0.FloatValue.Value, 5);
            Assert.Equal(new Dictionary<float, double> { { 1.1f, 2.2 } }, row0.FloatDoubleMap);
            Assert.Equal(new List<float> { 3.3f, 4.4f }, row0.FloatList);

            var row1 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.CollectionFilter.Eq(x => x.DoubleValue, double.NaN));
            Assert.True(double.IsNaN(row1.DoubleValue.Value));
            Assert.True(float.IsNaN(row1.FloatValue.Value));
            Assert.Equal(new Dictionary<float, double> { { float.NaN, double.NaN } }, row1.FloatDoubleMap);
            Assert.Equal(new List<float> { float.NaN }, row1.FloatList);

            var row2 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.CollectionFilter.Eq(x => x.DoubleValue, double.PositiveInfinity));
            Assert.True(double.IsPositiveInfinity(row2.DoubleValue.Value));
            Assert.Null(row2.FloatValue);
            Assert.Equal(new Dictionary<float, double> { { float.PositiveInfinity, double.NegativeInfinity } }, row2.FloatDoubleMap);
            Assert.Equal(new List<float> { float.PositiveInfinity }, row2.FloatList);

            var row3 = await typed.FindOneAsync(Builders<DoubleFloatTypeTest>.CollectionFilter.Eq(x => x.DoubleValue, double.NegativeInfinity));
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
    public async Task DateTime_Timezone_Tests()
    {
        var collectionName = "dateTimeTestCollection";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<CollectionDatetimeObject>(collectionName);
            var insertee = new CollectionDatetimeObject
            {
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
                new() { ["DoubleValue"] = "Infinity", ["FloatValue"] = "Infinity" },
                new() { ["DoubleValue"] = "-Infinity", ["FloatValue"] = "-Infinity" },
                new() { ["DoubleValue"] = "NaN", ["FloatValue"] = "NaN" },
            };

            var insertResult = await untyped.InsertManyAsync(okUntypedDocs);
            Assert.Equal(okUntypedDocs.Count, insertResult.InsertedIds.Count);

            foreach (var doc in okUntypedDocs) // so stupid there's no easy way to compare sets of dictionaries
            {
                var found = await untyped.FindOneAsync(Builders<Document>.CollectionFilter.Eq("DoubleValue", doc["DoubleValue"]));
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

    [Fact]
    public async Task Test_VectorEncodingCollection_Typed()
    {
        // Note: full check of this test involves manual log inspection for the payloads.
        var collectionName = "testColl_vecEncoding_Typed";
        try
        {
            var vecLstEncColl = fixture.Database.CreateCollection<VectorObjectAsLst>();
            var vecBinEncColl = fixture.Database.GetCollection<VectorObjectAsBin>(collectionName);

            var lstDocument = new VectorObjectAsLst
            {
                _id = "as_lst",
                TheVector = new float[] { 0.3f, -0.2f, 0.1f },
                TheBlob = Encoding.ASCII.GetBytes("a doc with a LST vector")
            };
            var binDocument = new VectorObjectAsBin
            {
                _id = "as_bin",
                TheVector = new float[] { 0.3f, -0.2f, 0.1f },
                TheBlob = Encoding.ASCII.GetBytes("a doc with a BIN vector")
            };

            await vecLstEncColl.InsertOneAsync(lstDocument);
            // all good with the payload for this write.
            await vecBinEncColl.InsertOneAsync(binDocument);

            // Reads:
            var lstReadAsLst = await vecLstEncColl.FindOneAsync(
                Builders<VectorObjectAsLst>.CollectionFilter.Eq(d => d._id, "as_lst"));
            var lstReadAsBin = await vecBinEncColl.FindOneAsync(
                Builders<VectorObjectAsBin>.CollectionFilter.Eq(d => d._id, "as_lst"));
            var binReadAsLst = await vecLstEncColl.FindOneAsync(
                Builders<VectorObjectAsLst>.CollectionFilter.Eq(d => d._id, "as_bin"));
            var binReadAsBin = await vecBinEncColl.FindOneAsync(
                Builders<VectorObjectAsBin>.CollectionFilter.Eq(d => d._id, "as_bin"));

            // all 'four' (two) vectors are the same values:
            Assert.Equal(lstReadAsLst.TheVector, lstReadAsBin.TheVector);
            Assert.Equal(lstReadAsLst.TheVector, binReadAsLst.TheVector);
            Assert.Equal(lstReadAsLst.TheVector, binReadAsBin.TheVector);

            // two different blobs must be read consistently
            Assert.Equal(lstReadAsLst.TheBlob, lstReadAsBin.TheBlob);
            Assert.Equal(binReadAsLst.TheBlob, binReadAsBin.TheBlob);

        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task Test_VectorEncodingCollection_Untyped()
    {
        // Note: full check of this test involves manual log inspection for the payloads.
        var collectionName = "testColl_vecEncoding_Untyped";
        try
        {
            var collDefinition = new CollectionDefinition { Vector = new VectorOptions { Dimension = 3 } };
            var createdCollection = await fixture.Database.CreateCollectionAsync(collectionName, collDefinition);

            await createdCollection.InsertOneAsync(new Document
                {
                    { "_id", "as_lst" },
                    { "$vector", new float[] { 0.3f, -0.2f, 0.1f } },
                    { "TheBlob", Encoding.ASCII.GetBytes("a doc with a LST vector") },
                });
            await createdCollection.InsertOneAsync(new Document
                {
                    { "_id", "as_bin" },
                    { "$vector", new Dictionary<string,string> { ["$binary"] = "PpmZmr5MzM09zMzN" } },
                    { "TheBlob", Encoding.ASCII.GetBytes("a doc with a BIN vector") },
                });

            // Reads:
            var findOptionsLst = new CollectionFindOneOptions<Document>()
            {
                Projection = Builders<Document>.Projection.Include("$vector") };
            var findOptionsBin = new CollectionFindOneOptions<Document>()
            {
                Projection = Builders<Document>.Projection.Include("$vector") };
            var lstRead = await createdCollection.FindOneAsync(
                Builders<Document>.CollectionFilter.Eq("_id", "as_lst"),
                findOptionsLst);
            var binRead = await createdCollection.FindOneAsync(
                Builders<Document>.CollectionFilter.Eq("_id", "as_bin"),
                findOptionsBin);

            // same values should be found for the vector (modulo some machine-precision epsilon)

            // ok for the LST reading (though these come out as doubles):
            // Assert.IsType<float>( ((object[])lstRead["$vector"]) [0]);
            var readVector = (Object[])lstRead["$vector"];
            Assert.Equal(3, readVector.Length);
            Assert.True( Math.Abs( 0.3 - (double)(readVector[0])) < 1.0e-5);
            Assert.True( Math.Abs(-0.2 - (double)(readVector[1])) < 1.0e-5);
            Assert.True( Math.Abs( 0.1 - (double)(readVector[2])) < 1.0e-5);

            // TODO the BIN read fails. Deserialization *should* figure out this is a $vector
            //      and decode, accordingly, into a `float[]`, even if untyped.
            //      Currently this comes back as [["$binary"] = PpmZmr5MzM09zMzN]
            // Assert.Equal(lstRead["$vector"], binRead["$vector"]); // check would still need an epsilon treatment maybe?

            // two different blobs must be read consistently
            // TODO these two fail. This is returned to the user as a `[["$binary"] = YSBkb2Mgd2l0aCBhIEJJTiB2ZWN0b3I=]` dict.
            //      Deserialization should figure out this is to become a `byte[]` and decode.
            // Assert.Equal(Encoding.ASCII.GetBytes("a doc with a LST vector"), lstRead["TheBlob"]);
            // Assert.Equal(Encoding.ASCII.GetBytes("a doc with a BIN vector"), binRead["TheBlob"]);

        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task Test_CollectionFindFilterSemantics()
    {
        var collectionName = "coll_findfiltersemantics";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithVector>(collectionName);
            await collection.InsertManyAsync(new List<SimpleObjectWithVector> {
                new SimpleObjectWithVector() { Id = 1, Name = "one" },
                new SimpleObjectWithVector() { Id = 2, Name = "two" }
            });

            // 'naked' findOne:
            // exp. payload: {"findOne":{}}
            var found_doc = await collection.FindOneAsync();
            Assert.NotNull(found_doc);

            // findOne through FILTER ONLY:
            //
            // exp. payload: {"findOne":{"filter":{"_id":{"$eq":1}}}}
            var find_f_id1 = await collection.FindOneAsync(Builders<SimpleObjectWithVector>.CollectionFilter.Eq(d => d.Id, 1));
            // exp. payload: {"findOne":{"filter":{"_id":{"$eq":2}}}}
            var find_f_id2 = await collection.FindOneAsync(Builders<SimpleObjectWithVector>.CollectionFilter.Eq(d => d.Id, 2));
            Assert.Equal("one", find_f_id1.Name);
            Assert.Equal("two", find_f_id2.Name);

            // findOne with filter parameter:
            //
            var filter_id1 = Builders<SimpleObjectWithVector>.CollectionFilter.Eq(d => d.Id, 1);
            var filter_id2 = Builders<SimpleObjectWithVector>.CollectionFilter.Eq(d => d.Id, 2);
            var findOptions = new CollectionFindOneOptions<SimpleObjectWithVector>();
            
            // exp. payload: {"findOne":{"filter":{"_id":{"$eq":1}}}}
            var find_o_id1 = await collection.FindOneAsync(filter_id1, findOptions);
            // exp. payload: {"findOne":{"filter":{"_id":{"$eq":2}}}}
            var find_o_id2 = await collection.FindOneAsync(filter_id2, findOptions);
            Assert.Equal("one", find_o_id1.Name);
            Assert.Equal("two", find_o_id2.Name);


        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    // Requires VOYAGE embedding provider
    [Fact(Skip="Should be run after exporting the environment variable quoted below")]
    public async Task Test_CollectionEmbeddingHeaders()
    {
        var embeddingAPIKey = Environment.GetEnvironmentVariable("HEADER_EMBEDDING_API_KEY_VOYAGEAI") ?? "kaboom";
        try
        {
            var collectionCr = await fixture.Database.CreateCollectionAsync<DocumentForEmbeddingHeaderTest>(
                new CreateCollectionOptions() {EmbeddingAPIKey = embeddingAPIKey}
            );
            await collectionCr.InsertOneAsync(new DocumentForEmbeddingHeaderTest() { Id = "a", Name = "Text for the a" });

            var collectionGe = fixture.Database.GetCollection<DocumentForEmbeddingHeaderTest>(
                new GetCollectionOptions() {EmbeddingAPIKey = embeddingAPIKey}
            );
            await collectionGe.InsertOneAsync(new DocumentForEmbeddingHeaderTest() { Id = "b", Name = "Text for the b" });

        }
        finally
        {
            await fixture.Database.DropCollectionAsync<DocumentForEmbeddingHeaderTest>();
        }
    }

    // Requires AWS (Bedrock) embedding provider
    [Fact(Skip="Should be run after exporting the environment variables quoted below")]
    public async Task Test_CollectionEmbeddingAWSHeaders()
    {
        // NOTE: make sure the region attribute in class DocumentForAWSEmbeddingHeaderTest matches the actual service being used.
        var embeddingAccessID = Environment.GetEnvironmentVariable("HEADER_EMBEDDING_ACCESS_ID_BEDROCK") ?? "kaboom";
        var embeddingSecretID = Environment.GetEnvironmentVariable("HEADER_EMBEDDING_SECRET_ID_BEDROCK") ?? "kaboom";
        try
        {
            var collectionCr = await fixture.Database.CreateCollectionAsync<DocumentForAWSEmbeddingHeaderTest>(
                new CreateCollectionOptions() {
                    AWSEmbeddingAPIKey = new () { AccessId = embeddingAccessID , SecretId = embeddingSecretID }
                }
            );
            await collectionCr.InsertOneAsync(new DocumentForAWSEmbeddingHeaderTest() { Id = "a", Name = "Text for the a" });

            var collectionGe = fixture.Database.GetCollection<DocumentForAWSEmbeddingHeaderTest>(
                new GetCollectionOptions() {
                    AWSEmbeddingAPIKey = new () { AccessId = embeddingAccessID , SecretId = embeddingSecretID }
                }
            );
            await collectionGe.InsertOneAsync(new DocumentForAWSEmbeddingHeaderTest() { Id = "b", Name = "Text for the b" });

            // ASIDE: test of getting the attribute:
            var nakedOptions = new CreateCollectionOptions() {};
            var richOptions = new CreateCollectionOptions() {
                AWSEmbeddingAPIKey = new () { AccessId = embeddingAccessID , SecretId = embeddingSecretID }
            };
            var awsKeyFromOptions = richOptions.AWSEmbeddingAPIKey;
            Assert.Null(nakedOptions.AWSEmbeddingAPIKey);
            Assert.IsType<AWSEmbeddingAPIKeyDescriptor>(awsKeyFromOptions);
            Assert.Equal(embeddingAccessID, awsKeyFromOptions.AccessId);
            Assert.Equal(embeddingSecretID, awsKeyFromOptions.SecretId);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync<DocumentForAWSEmbeddingHeaderTest>();
        }
    }

}

[CollectionName("testCollectionNameViaAttribute")]
public class CollectionNameObject
{
    public int? _id { get; set; }
    public string Test { get; set; }
}
