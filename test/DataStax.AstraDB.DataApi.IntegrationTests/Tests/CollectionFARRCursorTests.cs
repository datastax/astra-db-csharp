using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Enumeration;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.SerDes;
using System.Linq;
using MongoDB.Bson;
using System.Text;
using UUIDNext;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("CollectionFARRCursor")]
public class CollectionFARRCursorTests
{

    private readonly CollectionFARRCursorFixture _fixture;

    public CollectionFARRCursorTests(AssemblyFixture assemblyFixture, CollectionFARRCursorFixture fixture)
    {
        _fixture = fixture;
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorFARRCursor()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;
        var minValue = 3;

        var theFilter = Builders<FARRCursorTestVectorDocument>.CollectionFilter.Gt(d => d.PInt, minValue - 1);
        var findOptions = new CollectionFindAndRerankOptions<FARRCursorTestVectorDocument> {
            Sort = Builders<FARRCursorTestVectorDocument>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli"
        };

        var cur = filledCollection.FindAndRerank(theFilter, findOptions);
        var results = await cur.ToListAsync();

        Assert.NotNull(results);
        Assert.True(results.Count > 0);
        Assert.Equal(docCount - minValue + 1, results.Count);
        Assert.Equal($"text of document number {results[0].Document.PInt}", results[0].Document.PText);
        Assert.Equal($"doc_{results[0].Document.PInt}", results[0].Document.Id);
        Assert.Empty(results[0].Scores);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorizeFARRCursor()
    {
        var filledCollection = _fixture.FilledVectorizeCollection;
        var docCount = _fixture.FilledVectorizeCollectionCount;
        var minValue = 3;

        var theFilter = Builders<FARRCursorTestVectorizeDocument>.CollectionFilter.Gt(d => d.PInt, minValue - 1);

        // shorthand $hybrid sort
        var findOptionsShorthand = new CollectionFindAndRerankOptions<FARRCursorTestVectorizeDocument> {
            Sort = Builders<FARRCursorTestVectorizeDocument>.CollectionFindAndRerankSort.Hybrid("blabla")
        };

        var curSH = filledCollection.FindAndRerank(theFilter, findOptionsShorthand);
        var resultsSH = await curSH.ToListAsync();

        Assert.NotNull(resultsSH);
        Assert.True(resultsSH.Count > 0);
        Assert.Equal(docCount - minValue + 1, resultsSH.Count);
        Assert.Equal($"text of document number {resultsSH[0].Document.PInt}", resultsSH[0].Document.PText);
        Assert.Equal($"doc_{resultsSH[0].Document.PInt}", resultsSH[0].Document.Id);
        Assert.Empty(resultsSH[0].Scores);

        // explicit $lexical/$vectorize sort (to the same effect)
        var findOptionsExplicit = new CollectionFindAndRerankOptions<FARRCursorTestVectorizeDocument> {
            Sort = Builders<FARRCursorTestVectorizeDocument>.CollectionFindAndRerankSort.Hybrid(
                vectorize: "blabla_vectorize", lexical: "blabla_lexical"
            )
        };

        var curEX = filledCollection.FindAndRerank(theFilter, findOptionsExplicit);
        var resultsEX = await curEX.ToListAsync();

        Assert.NotNull(resultsEX);
        Assert.True(resultsEX.Count > 0);
        Assert.Equal(docCount - minValue + 1, resultsEX.Count);
        Assert.Equal($"text of document number {resultsEX[0].Document.PInt}", resultsEX[0].Document.PText);
        Assert.Equal($"doc_{resultsEX[0].Document.PInt}", resultsEX[0].Document.Id);
        Assert.Empty(resultsEX[0].Scores);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorFARR_IncludeSortVector()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;

        var theFilter = Builders<FARRCursorTestVectorDocument>.CollectionFilter.Empty();

        var findOptions = new CollectionFindAndRerankOptions<FARRCursorTestVectorDocument> {
            Sort = Builders<FARRCursorTestVectorDocument>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli",
            IncludeSortVector = true
        };

        var cur = filledCollection.FindAndRerank(theFilter, findOptions);
        var vpage0 = await cur.FetchNextPageAsync();
        Assert.Null(vpage0.NextPageState);
        Assert.Equal(docCount, vpage0.Results.Count);
        Assert.IsType<float[]>(vpage0.SortVector);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorFARR_IncludeScores()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;

        var theFilter = Builders<FARRCursorTestVectorDocument>.CollectionFilter.Empty();

        var findOptions = new CollectionFindAndRerankOptions<FARRCursorTestVectorDocument> {
            Sort = Builders<FARRCursorTestVectorDocument>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli",
            IncludeScores = true
        };

        var cur = filledCollection.FindAndRerank(theFilter, findOptions);
        var results = await cur.ToListAsync();
        Assert.True(results.Count > 0);
        Assert.NotNull(results[0].Scores["$rerank"]);
        Assert.NotNull(results[0].Scores["$vector"]);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorFARR_HybridLimits()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;
        var limitedCount = 2;

        var theFilter = Builders<FARRCursorTestVectorDocument>.CollectionFilter.Empty();

        var findOptionsHL = new CollectionFindAndRerankOptions<FARRCursorTestVectorDocument> {
            Sort = Builders<FARRCursorTestVectorDocument>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli",
            HybridLimits = limitedCount
        };

        var curHL = filledCollection.FindAndRerank(theFilter, findOptionsHL);
        var resultsHL = await curHL.ToListAsync();
        Assert.Equal(limitedCount, resultsHL.Count);

        var findOptionsVLL = new CollectionFindAndRerankOptions<FARRCursorTestVectorDocument> {
            Sort = Builders<FARRCursorTestVectorDocument>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli",
            VectorLimit = limitedCount,
            LexicalLimit = limitedCount
        };

        var curVLL = filledCollection.FindAndRerank(theFilter, findOptionsVLL);
        var resultsVLL = await curVLL.ToListAsync();
        Assert.Equal(limitedCount, resultsVLL.Count);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorFARR_Projection()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;

        var theFilter = Builders<FARRCursorTestVectorDocument>.CollectionFilter.Empty();

        var findOptions = new CollectionFindAndRerankOptions<FARRCursorTestVectorDocument> {
            Sort = Builders<FARRCursorTestVectorDocument>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli",
            Projection = Builders<FARRCursorTestVectorDocument>.Projection.Include("*")
        };

        var cur = filledCollection.FindAndRerank(theFilter, findOptions);
        var results = await cur.ToListAsync();
        Assert.Equal(docCount, results.Count);
        var doc0 = results[0].Document;

        Assert.IsType<string>(doc0.Id);
        Assert.IsType<string>(doc0.PText);
        Assert.IsType<int>(doc0.PInt);
        Assert.IsType<float[]>(doc0.Vector);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorizeFARR_Projection()
    {
        var filledCollection = _fixture.FilledVectorizeCollection;
        var docCount = _fixture.FilledVectorizeCollectionCount;

        var theFilter = Builders<FARRCursorTestVectorizeDocument>.CollectionFilter.Empty();

        var findOptions = new CollectionFindAndRerankOptions<FARRCursorTestVectorizeDocument> {
            Sort = Builders<FARRCursorTestVectorizeDocument>.CollectionFindAndRerankSort.Hybrid("blabla"),
            Projection = Builders<FARRCursorTestVectorizeDocument>.Projection.Include("*")
        };

        var cur = filledCollection.FindAndRerank(theFilter, findOptions);
        var results = await cur.ToListAsync();
        Assert.Equal(docCount, results.Count);
        var doc0 = results[0].Document;

        Assert.IsType<string>(doc0.Id);
        Assert.IsType<string>(doc0.PText);
        Assert.IsType<int>(doc0.PInt);
        Assert.IsType<string>(doc0.Vectorize);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorFARR_FluentInterface()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;
        var minValue = 3;

        var cur = filledCollection.FindAndRerank()
            .Filter(Builders<FARRCursorTestVectorDocument>.CollectionFilter.Gt(d => d.PInt, minValue - 1))
            .Limit(docCount - minValue + 1)
            .Project(Builders<FARRCursorTestVectorDocument>.Projection.Include(doc => doc.PInt))
            .IncludeSortVector(true)
            .Sort(Builders<FARRCursorTestVectorDocument>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"))
            .RerankOn(d => d.PText)  // .RerankOn("$lexical") is also OK
            .RerankQuery("blibli")
            .HybridLimits(vectorLimit: 20, lexicalLimit: 20)
            .IncludeScores(true);

        var results = await cur.ToListAsync();
        Assert.Equal(docCount - minValue + 1, results.Count);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorUntypedFARRCursor()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;

        var untypedCollection = _fixture.Database.GetCollection(filledCollection.CollectionName);

        var theFilter = Builders<Document>.CollectionFilter.Eq("PInt", 2);
        var findOptions = new CollectionFindAndRerankOptions<Document> {
            Sort = Builders<Document>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli"
        };

        var cur = untypedCollection.FindAndRerank(theFilter, findOptions);
        var results = await cur.ToListAsync();

        Assert.NotNull(results);
        Assert.Equal(1, results.Count);
        var document = results[0].Document;

        Assert.Equal("doc_2", document["_id"]);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task Test_CollectionVectorFARRCursor_RerankingHeader()
    {
        var filledCollection = _fixture.FilledVectorCollection;
        var docCount = _fixture.FilledVectorCollectionCount;

        var untypedCollection = _fixture.Database.GetCollection(
            filledCollection.CollectionName,
            new GetCollectionOptions() { RerankingAPIKey = "a-very-wrong-reranking-key" }
        );

        var theFilter = Builders<Document>.CollectionFilter.Eq("PInt", 2);
        var findOptions = new CollectionFindAndRerankOptions<Document> {
            Sort = Builders<Document>.CollectionFindAndRerankSort.Hybrid(new float[] {1.0f, 0.0f}, "blabla"),
            RerankOn = "$lexical",
            RerankQuery = "blibli"
        };

        var cur = untypedCollection.FindAndRerank(theFilter, findOptions);

        await Assert.ThrowsAsync<CommandException>(async () => await cur.ToListAsync());
    }

}
