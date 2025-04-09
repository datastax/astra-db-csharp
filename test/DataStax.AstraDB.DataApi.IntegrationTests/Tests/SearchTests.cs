using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using MongoDB.Bson;
using System.Linq;
using System.Text.Json;
using UUIDNext;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("DatabaseAndCollections")]
public class SearchTests
{
    CollectionsFixture fixture;

    public SearchTests(CollectionsFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task ById()
    {
        var collectionName = "idsSearchCollection";

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
                },
                new DifferentIdsObject()
                {
                    TheId = date,
                    Name = $"Test Object DateTime"
                }
            };

            var collection = await fixture.Database.CreateCollectionAsync<DifferentIdsObject>(collectionName);
            await collection.InsertManyAsync(items, new InsertManyOptions() { InsertInOrder = true });

            //Search using Expression
            var filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, 1);
            var searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(1.ToString(), searchResult.TheId.ToString());
            Assert.Equal("Test Object Int", searchResult.Name);

            //Search using String
            filter = Builders<DifferentIdsObject>.Filter.Eq("_id", 1);
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(1.ToString(), searchResult.TheId.ToString());
            Assert.Equal("Test Object Int", searchResult.Name);

            //objectId
            filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, objectId);
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(objectId.ToString(), searchResult.TheId.ToString());
            Assert.Equal("Test Object ObjectId", searchResult.Name);

            //uuid4
            filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, uuid4);
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(uuid4.ToString(), searchResult.TheId.ToString());
            Assert.Equal("Test Object UUID4", searchResult.Name);

            //uuid5
            filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, uuid5);
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(uuid5.ToString(), searchResult.TheId.ToString());
            Assert.Equal("Test Object UUID5", searchResult.Name);

            //uuid7
            filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, uuid7);
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(uuid7.ToString(), searchResult.TheId.ToString());
            Assert.Equal("Test Object UUID7", searchResult.Name);

            //uuid8
            filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, uuid8);
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(uuid8.ToString(), searchResult.TheId.ToString());
            Assert.Equal("Test Object UUID8", searchResult.Name);

            //string
            filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, "This is an id string");
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal("This is an id string", searchResult.TheId.ToString());
            Assert.Equal("Test Object String", searchResult.Name);

            //date
            filter = Builders<DifferentIdsObject>.Filter.Eq(d => d.TheId, date);
            searchResult = await collection.FindOneAsync(filter);
            Assert.Equal(date.ToUniversalTime().ToString("MMddyyhhmmss"), ((DateTime)searchResult.TheId).ToUniversalTime().ToString("MMddyyhhmmss"));
            Assert.Equal("Test Object DateTime", searchResult.Name);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public void SimpleStringFilter()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq("Properties.PropertyOne", "grouptwo");
        var results = collection.Find(filter).ToList();
        var expectedArray = new[] { "horse", "cow", "alligator" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void SimpleExpressionFilter()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq(so => so.Properties.PropertyOne, "grouptwo");
        var results = collection.Find(filter).ToList();
        var expectedArray = new[] { "horse", "cow", "alligator" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void Limit_RunsAsync_ReturnsLimitedResult()
    {
        var collection = fixture.SearchCollection;
        var results = collection.Find().Limit(1).ToList();
        Assert.Single(results);
    }

    [Fact]
    public void InclusiveProjection_RunsSync_ReturnsIncludedProperties()
    {
        var collection = fixture.SearchCollection;
        var inclusiveProjection = Builders<SimpleObject>.Projection
                .Include("Properties.PropertyTwo");
        var results = collection.Find().Limit(1).Project(inclusiveProjection).ToList();
        var result = results.First();
        Assert.True(string.IsNullOrEmpty(result.Name));
        Assert.True(string.IsNullOrEmpty(result.Properties.PropertyOne));
        Assert.False(string.IsNullOrEmpty(result.Properties.PropertyTwo));
    }

    [Fact]
    public void ExclusiveProjection_RunsSync_ExcludesProperties()
    {
        var collection = fixture.SearchCollection;
        var exclusiveProjection = Builders<SimpleObject>.Projection
                .Exclude("Properties.PropertyTwo");
        var results = collection.Find().Limit(1).Project(exclusiveProjection).ToList();
        var result = results.First();
        Assert.False(string.IsNullOrEmpty(result.Name));
        Assert.False(string.IsNullOrEmpty(result.Properties.PropertyOne));
        Assert.True(string.IsNullOrEmpty(result.Properties.PropertyTwo));
    }

    [Fact]
    public void Sort_RunsAsync_ReturnsSortedResult()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq(so => so.Properties.PropertyOne, "grouptwo");
        var sort = Builders<SimpleObject>.Sort.Ascending(o => o.Properties.PropertyTwo);
        var results = collection.Find(filter).Sort(sort).ToList();
        var expectedArray = new[] { "alligator", "cow", "horse" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void SortDescending_RunsAsync_ReturnsResultsDescending()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq(so => so.Properties.PropertyOne, "grouptwo");
        var sort = Builders<SimpleObject>.Sort.Descending(o => o.Properties.PropertyTwo);
        var results = collection.Find(filter).Sort(sort).ToList();
        var expectedArray = new[] { "horse", "cow", "alligator" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void Skip_RunsAsync_ReturnsResultsAfterSkip()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq(so => so.Properties.PropertyOne, "grouptwo");
        var sort = Builders<SimpleObject>.Sort.Descending(o => o.Properties.PropertyTwo);
        var results = collection.Find(filter).Sort(sort).Skip(1).ToList();
        var expectedArray = new[] { "cow", "alligator" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void LimitAndSkip_RunsAsync_ReturnsExpectedResults()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq(so => so.Properties.PropertyOne, "grouptwo");
        var sort = Builders<SimpleObject>.Sort.Descending(o => o.Properties.PropertyTwo);
        var results = collection.Find(filter).Sort(sort).Skip(2).Limit(1).ToList();
        var expectedArray = new[] { "alligator" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void NotFluent_RunsAsync_ReturnsExpectedResults()
    {
        var collection = fixture.SearchCollection;
        var filter = Builders<SimpleObject>.Filter.Eq(so => so.Properties.PropertyOne, "grouptwo");
        var sort = Builders<SimpleObject>.Sort.Descending(o => o.Properties.PropertyTwo);
        var inclusiveProjection = Builders<SimpleObject>.Projection
                .Include("Properties.PropertyTwo");
        var findOptions = new FindOptions<SimpleObject>()
        {
            Sort = sort,
            Limit = 1,
            Skip = 2,
            Projection = inclusiveProjection
        };
        var results = collection.Find(filter, findOptions).ToList();
        var expectedArray = new[] { "alligator" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
        var result = results.First();
        Assert.True(string.IsNullOrEmpty(result.Name));
        Assert.True(string.IsNullOrEmpty(result.Properties.PropertyOne));
        Assert.False(string.IsNullOrEmpty(result.Properties.PropertyTwo));
    }

    [Fact]
    public void LogicalAnd_MongoStyle()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Eq(so => so.Properties.PropertyOne, "grouptwo") & builder.Eq(so => so.Properties.PropertyTwo, "cow");
        var results = collection.Find(filter).ToList();
        var expectedArray = new[] { "cow" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void LogicalAnd_AstraStyle()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.And(builder.Eq(so => so.Properties.PropertyOne, "grouptwo"), builder.Eq(so => so.Properties.PropertyTwo, "cow"));
        var results = collection.Find(filter).ToList();
        var expectedArray = new[] { "cow" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void LogicalOr_MongoStyle()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Eq(so => so.Properties.PropertyTwo, "alligator") | builder.Eq(so => so.Properties.PropertyTwo, "cow");
        var sort = Builders<SimpleObject>.Sort.Ascending(o => o.Properties.PropertyTwo);
        var results = collection.Find(filter).Sort(sort).ToList();
        var expectedArray = new[] { "alligator", "alligator", "cow", "cow" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public void LogicalOr_AstraStyle()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Or(builder.Eq(so => so.Properties.PropertyOne, "groupone"), builder.Eq(so => so.Properties.PropertyOne, "grouptwo"));
        var sort = Builders<SimpleObject>.Sort.Ascending(o => o.Properties.PropertyTwo);
        var results = collection.Find(filter).Sort(sort).ToList();
        var expectedArray = new[] { "alligator", "cat", "cow", "dog", "horse" };
        var actualArray = results.Select(o => o.Properties.PropertyTwo).ToArray();
        Assert.True(!expectedArray.Except(actualArray).Any() && !actualArray.Except(expectedArray).Any());
    }

    [Fact]
    public async Task TestAsyncEnumeration()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Or(builder.Eq(so => so.Properties.PropertyOne, "groupone"), builder.Eq(so => so.Properties.PropertyOne, "grouptwo"));
        var sort = Builders<SimpleObject>.Sort.Ascending(o => o.Properties.PropertyTwo);
        var results = collection.Find(filter).Sort(sort);
        var expectedArray = new[] { "alligator", "cat", "cow", "dog", "horse" };
        var resultPropertyTwos = new List<string>();
        await foreach (var result in results)
        {
            resultPropertyTwos.Add(result.Properties.PropertyTwo);
        }
        Assert.True(!expectedArray.Except(resultPropertyTwos).Any() && !resultPropertyTwos.Except(expectedArray).Any());
    }

    [Fact]
    public async Task FindAll_AsyncEnumeration()
    {
        var collection = fixture.SearchCollection;
        var results = collection.Find();
        var names = new List<string>();
        await foreach (var result in results)
        {
            names.Add(result.Name);
        }
        Assert.Equal(33, names.Count);
    }

    [Fact]
    public void LogicalNot_MongoStyle()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = !(builder.Eq(so => so.Properties.PropertyTwo, "alligator") | builder.Eq(so => so.Properties.PropertyTwo, "cow"));
        var results = collection.Find(filter).ToList();
        Assert.Equal(29, results.Count);
    }

    [Fact]
    public void LogicalNot_AstraStyle()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Not(builder.Eq(so => so.Properties.PropertyTwo, "alligator") | builder.Eq(so => so.Properties.PropertyTwo, "cow"));
        var results = collection.Find(filter).ToList();
        Assert.Equal(29, results.Count);
    }

    [Fact]
    public void GreaterThan()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Gt(so => so.Properties.IntProperty, 20);
        var sort = Builders<SimpleObject>.Sort.Ascending(o => o.Properties.IntProperty);
        var results = collection.Find(filter).Sort(sort).ToList();
        Assert.Equal(13, results.Count);
        Assert.Equal(21, results.First().Properties.IntProperty);
    }

    [Fact]
    public void GreaterThan_Date()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Gt(so => so.Properties.DateTimeProperty, new DateTime(2020, 1, 1, 1, 14, 0));
        var sort = Builders<SimpleObject>.Sort.Ascending(o => o.Properties.DateTimeProperty);
        var results = collection.Find(filter).Sort(sort).ToList();
        Assert.Equal(19, results.Count);
        Assert.Equal(new DateTime(2020, 1, 1, 1, 15, 0).ToUniversalTime().ToString("MMddyyhhmmss"), results.First().Properties.DateTimeProperty.ToUniversalTime().ToString("MMddyyhhmmss"));
    }

    [Fact]
    public void GreaterThanOrEqual()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Gte(so => so.Properties.IntProperty, 20);
        var sort = Builders<SimpleObject>.Sort.Ascending(o => o.Properties.IntProperty);
        var results = collection.Find(filter).Sort(sort).ToList();
        Assert.Equal(14, results.Count);
        Assert.Equal(20, results.First().Properties.IntProperty);
    }

    [Fact]
    public void LessThan()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Lt(so => so.Properties.IntProperty, 20);
        var sort = Builders<SimpleObject>.Sort.Descending(o => o.Properties.IntProperty);
        var results = collection.Find(filter).Sort(sort).ToList();
        Assert.Equal(19, results.Count);
        Assert.Equal(19, results.First().Properties.IntProperty);
    }

    [Fact]
    public void LessThanOrEqual()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Lte(so => so.Properties.IntProperty, 20);
        var sort = Builders<SimpleObject>.Sort.Descending(o => o.Properties.IntProperty);
        var results = collection.Find(filter).Sort(sort).ToList();
        Assert.Equal(20, results.Count);
        Assert.Equal(20, results.First().Properties.IntProperty);
    }

    [Fact]
    public void NotEqualTo()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Ne(so => so.Properties.PropertyOne, "groupthree");
        var results = collection.Find(filter).ToList();
        Assert.Equal(7, results.Count);
    }

    [Fact]
    public void InArray()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.In(so => so.Properties.PropertyOne, new[] { "groupone", "grouptwo" });
        var results = collection.Find(filter).ToList();
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public void NotInArray()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Nin(so => so.Properties.PropertyOne, new[] { "groupone", "grouptwo" });
        var results = collection.Find(filter).ToList();
        Assert.Equal(28, results.Count);
    }

    [Fact]
    public void InArray_WithArrays()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.In(so => so.Properties.StringArrayProperty, new[] { "cat1", "dog1" });
        var results = collection.Find(filter).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task PropertyExists()
    {
        var collectionName = "differentProperties";
        try
        {

            List<SimpleObjectSkipNulls> items = new List<SimpleObjectSkipNulls>();
            for (var i = 0; i < 5; i++)
            {
                items.Add(new SimpleObjectSkipNulls()
                {
                    _id = i,
                    Name = $"Test Object {i}",
                    PropertyOne = i % 2 == 0 ? "groupone" : "grouptwo",
                    PropertyTwo = i % 2 == 0 ? "hasvalue" : null
                });
            }
            ;
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectSkipNulls>(collectionName);
            var result = await collection.InsertManyAsync(items);
            var builder = Builders<SimpleObjectSkipNulls>.Filter;
            var filter = builder.Exists(so => so.PropertyTwo);
            var results = collection.Find(filter).ToList();
            Assert.Equal(3, results.Count);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public void ArrayContainsAll()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.All(so => so.Properties.StringArrayProperty, new[] { "alligator1", "alligator2", "alligator3" });
        var results = collection.Find(filter).ToList();
        Assert.Single(results);
    }

    [Fact]
    public void ArrayHasSize()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Size(so => so.Properties.StringArrayProperty, 3);
        var results = collection.Find(filter).ToList();
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task QueryDocumentsWithVectorsAsync()
    {
        var collectionName = "simpleObjectsWithVectors";
        try
        {
            List<SimpleObjectWithVector> items = new List<SimpleObjectWithVector>() {
                new()
                {
                    Id = 0,
                    Name = "This is about a cat.",
                    VectorEmbeddings = (new double[] { 0.089152224,0.041651253,0.006238773,0.043506123,-0.08882473,-0.028041942,0.057035644,0.021510456,-0.0335324,-0.015038743,-0.010946267,-0.033052385,-0.004451128,0.024450185,-0.10682846,-0.008512233,-0.06361482,0.011363489,-0.018674858,0.05755979,0.010276637,0.07108201,0.02609113,0.0051583643,-0.099871725,0.064882025,-0.04566266,-0.0447845,0.015848588,0.05461174,-0.09496783,0.00975987,-0.0068527046,0.058530692,-0.077218644,-0.07914207,0.015511969,0.069497555,0.065023474,0.08405613,-0.009922312,-0.055714585,0.014438505,-0.013197334,-0.057995494,0.034810554,-0.0021572637,-0.07460552,0.05961566,-0.06183412,-0.079871744,-0.011298,-0.049480535,-0.024539579,-0.021874059,-0.0039382433,0.051196072,-0.048952673,-0.026021041,-0.016276214,0.02319573,0.041072488,0.008482071,0.09586513,0.038031645,-0.024842998,0.003575021,-0.011547007,0.006391744,-0.049402386,0.0467421,0.00959334,-0.03264049,0.026778983,-0.009813442,-0.06520412,0.07849598,-0.0003208971,0.0959958,0.0781274,-0.042293962,-0.014687874,0.03163216,0.07175497,0.008732745,0.06964863,0.031615674,-0.05005147,-0.047878552,-0.0005746399,-0.006433992,-0.020098396,-0.0030210759,0.0011368011,-0.0495901,-0.011242182,-0.003216857,-0.06091114,-0.025426276,0.076356426,0.023855325,-0.0024857672,0.036267877,0.07782738,-0.035250574,-0.0038455587,0.02195504,0.03382706,0.021517783,0.0117069045,0.028399047,-0.10053743,-0.05483047,0.076560654,0.08378389,0.005169428,0.022630876,-0.051714625,0.06101812,-0.012016718,0.08596926,-0.045620713,-0.0357822,0.026691968,-0.00868368,-0.06845361,-0.0064422367,-1.0561074e-32,0.018288853,-0.03167005,0.01725241,-0.016210299,0.103539616,0.023504676,-0.013519004,0.003982343,-0.029885074,0.033861946,-0.09327008,-0.05178391,-0.0436475,-0.059817906,-0.009489782,-0.08695476,-0.0659479,-0.005502922,-0.0057627205,0.03876865,0.011172102,0.08398976,0.060095742,-0.032664955,-0.0024455208,-0.06746624,-0.080734365,-0.079751015,-0.023598457,0.0002563353,0.053381264,0.026176691,0.07066675,-0.020418348,-0.11429579,-0.047344334,-0.013287097,-0.01864271,-0.0063428497,0.037078038,0.043008555,-0.004289443,0.03631616,0.033966597,0.022350233,0.031604636,-0.024446608,0.019221745,-0.046859663,0.060180783,0.12533426,-0.00479271,0.035111945,-0.040028594,-0.031212045,0.010197592,0.030907258,-0.030999627,-0.007732326,0.13904367,0.04940509,0.010669046,0.016782433,-0.025963195,0.023240745,-0.063346595,0.019595988,-0.021747895,0.089717746,0.07910935,-0.0657273,0.019861227,0.03558573,-0.086711794,-0.016101884,-0.019212069,0.02883235,-0.011353625,-0.08117944,0.08045923,-0.03617811,-0.015651839,0.11304874,-0.011179125,-0.03710327,0.041835453,0.02170425,-0.035943862,-0.059953246,0.07860478,-0.083585985,0.08917205,-0.027793054,-0.1512843,0.06380901,6.228111e-33,-0.026383527,-0.0017929141,0.023303084,0.02218534,-0.06251773,0.048760373,0.010181715,0.07173921,-0.07172141,0.07956989,-0.04191949,0.07540622,0.12423252,-0.0070834314,0.09814076,0.039843906,-0.018787384,0.007695833,0.0671187,-0.03075603,-0.04254027,0.05959335,-0.06380347,0.038309086,0.023116969,0.014789075,0.031658806,-0.055602986,-0.0071550673,-0.19415864,0.01743854,-0.13140139,-0.0144067705,-0.04930145,-0.050060764,0.057865113,-0.053838655,-0.09110394,-0.022664431,-0.014732251,0.0136811845,-0.008339691,0.043517523,-0.0043350235,-0.0198879,-0.037622113,-0.0013067131,-0.009471046,-0.0052737775,0.032179937,-0.03113369,-0.09677153,0.035852194,0.01569161,0.028246677,0.009729108,-0.03177169,-0.009778319,0.017784022,0.032719884,-0.009280669,0.030814694,-0.033533014,0.052525237,-0.06690553,-0.02933622,-0.02315666,-0.0909028,0.009725973,-0.056958094,0.12035951,0.03881476,-0.072808184,-0.07154062,-0.014807507,-0.0012886004,0.053991668,-0.022750335,-0.031523474,-0.05695466,-0.019918608,-0.041242998,0.008761982,0.011521017,0.021141063,-0.032588396,0.027337488,0.092536,-0.019304343,0.05793468,-0.0062565147,0.0122958785,0.034887247,-0.120099336,0.013632101,-1.8641153e-8,-0.03772457,-0.048523452,-0.097976536,0.021443354,0.024639249,0.055306807,0.039979752,-0.1179702,-0.05120052,0.012400957,0.04489748,-0.015811147,0.015597527,0.082042284,0.052555256,0.055382337,0.00070087315,-0.019393984,0.01994989,0.11038251,-0.07126654,-0.020937268,-0.057667337,0.0013548834,-0.028954726,-0.0112794535,-0.013444409,0.08701174,-0.011877562,-0.077941716,0.039188765,-0.006396562,0.020704756,-0.050458863,0.0053959913,-0.037047394,0.07261843,-0.11010277,0.03790635,-0.055131394,0.001222146,0.00043609156,0.083624594,0.0116417315,-0.039805945,0.025670456,0.023367377,-0.030002289,0.0125302505,0.055243578,0.021209959,0.10720353,0.061538137,0.028139735,-0.0011503869,-0.033695076,0.0071400655,-0.011627619,0.018046776,0.06495565,0.038648676,0.062729195,0.055372205,0.053745285 }).Select(d => (float)d).ToArray()
                },
                new()
                {
                    Id = 1,
                    Name = "This is about a dog.",
                    VectorEmbeddings = (new double[] { 0.011346418,0.04229269,0.03587526,0.06744778,-0.04397914,-0.015584227,0.041720465,0.014429322,0.044287052,0.0043079043,0.019921772,-0.043505132,0.043102365,0.047135014,-0.08231162,0.020834573,-0.02617832,0.006700209,-0.012063026,-0.022996914,-0.019173175,0.07027755,0.015753163,-0.005731548,-0.1283017,0.022473892,0.0108027095,-0.04926471,0.036433354,0.020997025,-0.060065117,0.008349197,-0.018449424,0.035899807,-0.068035096,-0.06751048,0.03407754,0.09698124,0.08279569,0.0680067,0.050136272,-0.01190681,0.0332499,-0.039791144,-0.03364588,0.030574504,-0.09330098,-0.05924064,0.070340574,-0.025546642,-0.048177682,-0.024846006,-0.040927712,-0.02904687,-0.041816615,-0.04117641,-0.040064435,-0.009248488,-0.019651065,-0.020427428,0.028331814,0.04488185,0.039839122,0.03350426,-0.0030064413,-0.0049702036,-0.023976147,-0.023749182,-0.053586725,-0.03215982,0.06093219,0.012142569,0.0441472,-0.004723474,-0.010604439,-0.122735985,-0.0122516565,-0.0027201625,0.15101196,0.07122502,-0.056468617,-0.034811575,-0.0425355,0.068009876,0.016610937,0.033443205,0.02710159,-0.042842995,-0.08505483,-0.008643339,-0.068027355,-0.06379054,-0.029316818,0.0058006127,-0.030307215,-0.008950219,-0.047879178,-0.0913828,-0.009465094,0.053881057,0.0031493043,0.025003623,0.07674008,0.015249092,0.04850128,-0.012600311,-0.026310094,0.09720545,0.01680918,0.013852205,-0.018736323,-0.06005147,-0.020879472,0.07370358,0.09260949,0.021539116,-0.055899676,-0.05503345,-0.015129937,-0.024785034,0.08288727,-0.013179343,-0.0726965,0.020562222,0.022948798,-0.034667227,0.0526193,-1.18245034e-32,0.050559655,-0.0048819673,0.0063548665,-0.05116284,0.008868584,-0.0021307247,0.01315732,0.002422845,-0.053024683,0.043829612,-0.032446425,-0.034506686,-0.0022936491,-0.009842915,0.091232926,-0.077086814,-0.031722493,0.015017364,0.06814536,-0.0017685982,-0.017744187,0.073336154,0.019819103,-0.0644845,0.0034368993,0.005495123,-0.084544875,-0.06872212,-0.059192233,0.022209745,0.038755916,0.03318034,0.07090409,-0.04171937,-0.0981389,0.012770359,-0.009513227,-0.080934696,-0.033764116,0.05497646,0.08185176,-0.029122388,0.0384573,0.021990445,-0.0032139954,0.041740686,-0.056978893,-0.04734529,-0.03991311,0.019610234,0.0647751,0.004388865,0.10982947,-0.043376997,0.010122793,0.032659534,0.055898506,0.0195673,-0.009158063,0.123804346,0.0708504,-0.0071747694,-0.036037583,-0.049208198,0.058279518,-0.0790463,-0.12674646,-0.04109742,-0.038129613,0.07686497,-0.04693619,-0.012978095,0.06309132,-0.04087212,0.019872552,0.010217363,0.08006997,0.0026933537,0.001853705,-0.004593111,-0.048401523,0.013331191,0.036138237,-0.0344762,-0.018189447,0.00083567185,0.020371282,0.027441347,-0.054629076,0.008875433,-0.052847195,-0.030010821,-0.035492633,-0.070175104,0.054558296,6.417459e-33,-0.008761953,0.06607552,0.026014887,0.05661787,-0.011050351,0.004505302,-0.02256045,0.12539902,-0.08650192,0.061125778,-0.042015005,0.02355448,0.08238224,-0.005603566,0.09459545,0.0948628,0.017250946,0.033425502,0.040046655,-0.0628745,-0.1046111,0.065641314,0.042201586,0.0120828645,-0.04438299,0.07246842,0.02128908,-0.08484302,-0.041710783,-0.11470048,-0.053501517,-0.117971286,-0.00788975,-0.055501066,-0.05120677,0.06829499,0.031432264,-0.12799612,-0.011093621,-0.019008748,0.058515273,-0.061318677,0.037527867,0.014019596,0.060755204,0.03810258,0.0096842535,-0.04321577,0.00712279,0.046378646,-0.047010764,-0.042070884,0.04731553,-0.024275718,0.02193629,-0.018049095,-0.09782932,0.0030079312,0.041306987,0.007830905,-0.013250943,0.07706189,-0.026540434,0.093507566,-0.10470637,-0.05415237,-0.023200897,-0.039328523,0.03200482,-0.08281106,0.041430682,0.04234739,-0.03488221,-0.05283925,0.014213709,0.0029994198,0.027753718,-0.052510686,0.018441025,0.0052146562,-0.08947327,-0.05556312,-0.026160069,0.011457789,0.029503262,0.0025367353,-0.028680122,0.10300588,0.0031025852,0.015188665,-0.023412446,0.06668626,0.00421559,-0.09940255,0.020253142,-1.9181371e-8,-0.033001088,-0.013900804,-0.04732987,-0.002962324,0.06098004,0.066006675,0.03184782,-0.11235283,-0.055892687,0.019401163,0.04267367,-0.022931533,-0.07635768,0.058402903,0.013820379,0.04567165,-0.009838069,0.041856937,0.07559799,0.10492714,-0.062181033,-0.034038723,0.056748517,-0.045047298,-0.037589937,-0.036176957,0.013583488,0.07220805,-0.11159318,-0.016334815,-0.02307086,0.08323495,-0.043899763,-0.015546188,0.006285,-0.06952057,0.10265458,-0.068090186,-0.012350388,-0.001622836,0.013450635,0.09277687,-0.008574189,0.015505242,0.06175743,0.097206704,0.051632088,-0.05997204,-0.0013132367,-0.028132863,-0.09432876,0.0059271497,0.061715312,0.0057019778,-0.008623754,-0.016418574,-0.017792245,-0.066690564,0.040184673,0.051649246,-0.0033247564,0.071918964,0.02674936,0.07687916 }).Select(d => (float)d).ToArray()
                },
                new()
                {
                    Id = 2,
                    Name = "This is about a horse.",
                    VectorEmbeddings = (new double[] { 0.00066319015,0.059371606,0.019904254,0.036774546,-0.06027116,-0.016265921,0.030860845,0.055643834,0.0042737657,-0.007037373,0.006104062,-0.042017885,0.03532925,-0.01983578,-0.17182247,0.018130027,0.0006978096,0.041010544,-0.04580896,0.0066562407,-0.034156695,0.07169789,-0.003732026,0.07035341,-0.06771691,-0.045483362,-0.04587486,0.044270802,-0.039063204,0.045345873,-0.067072794,-0.036293034,0.012792531,-0.06584763,-0.15003937,0.006887971,0.03122762,0.071715415,0.060147583,0.056536376,0.1005726,-0.07508944,-0.010521994,0.04296306,0.06186383,0.048336152,-0.0026578992,-0.031771813,0.09114186,0.0298219,-0.030069979,-0.04830448,-0.06555674,0.008931567,-0.008930034,0.008409578,-0.06380391,0.031089691,-0.022144161,0.0031081801,0.027430866,0.053916246,0.03481386,0.06882019,-0.040121544,0.053177223,-0.08955376,-0.025653286,-0.061899573,-0.056186076,0.0618388,-0.041082207,0.0024167337,-0.10221955,-0.036011945,-0.02193175,-0.027966196,-0.0052382657,0.12434561,0.036994625,-0.044019878,-0.07336038,0.004784511,-0.014793327,0.05463077,0.03792995,0.052015834,-0.12259907,-0.083660625,-0.05770183,-0.05553689,-0.015968101,-0.035130754,0.015152355,0.04673879,0.02763963,0.0045751594,0.00013351876,-0.05073031,0.069344886,0.050685488,-0.006217909,0.057289913,-0.008384084,-0.010095412,0.04262858,-0.076505005,0.004814693,-0.004647454,-0.027164018,-0.052793253,-0.07996781,-0.002259928,0.05102953,0.07469597,0.047816664,-0.11623857,0.009546492,-0.07836857,-0.0061497106,0.030308023,0.0123279765,0.01727557,-0.027787533,0.0029694655,-0.048251797,0.05783699,-9.8458165e-33,-0.029556712,-0.059958287,0.020482214,-0.055502545,0.012202032,-0.019966707,0.0063604047,0.012528869,-0.023461204,0.04148049,0.010254733,-0.079121485,0.005512616,-0.050672587,0.0969294,-0.03734898,-0.016416196,-0.011727342,0.019440984,-0.0103121335,-0.004015404,0.10709091,-0.0042037196,-0.09906271,-0.020815263,-0.062631406,-0.05181335,-0.056682684,-0.07668673,0.04462328,-0.03255967,-0.01061319,0.040095136,-0.07428344,-0.08954705,-0.03655255,-0.033414897,-0.07287172,-0.046245173,0.040741507,0.043121435,-0.017704992,0.04531322,0.022490712,-0.061796345,0.12263498,0.023731327,0.04097611,-0.02944033,0.027172178,0.054726616,-0.009525099,0.08476988,0.012729447,0.026644293,0.065845735,0.01771869,0.049338885,-0.052020647,0.011609476,0.05302537,0.03191476,-0.011428133,-0.0317218,0.000072717354,-0.060584724,-0.114904806,0.020161727,0.023225058,0.059694566,-0.02951997,0.03259462,-0.04916039,-0.074076205,-0.027767904,0.0047858614,0.07736213,-0.08744972,-0.03654403,-0.01477788,-0.09468339,0.0018939535,-0.0182993,-0.036130246,0.01050344,0.033560347,0.019387867,-0.02084657,0.0009360842,0.013718448,0.046229977,0.024833633,-0.11396671,-0.10177031,0.055133626,6.0249805e-33,0.01816002,0.052471697,0.054190416,0.11878632,0.03944209,-0.046787247,0.032814603,0.058386937,-0.049842246,0.049228907,-0.066142194,0.006059039,0.059621867,-0.006291052,0.15240562,0.025465403,0.032288413,0.03136309,0.07285025,-0.041628312,-0.01245005,0.035763763,0.0012085717,-0.031110683,-0.04029571,0.07998812,-0.061727177,-0.06920058,-0.023586895,-0.06451264,-0.067086466,-0.057362735,-0.011721348,-0.010412064,-0.084145345,0.04934646,0.04814221,-0.08387328,0.018630628,-0.009486296,0.11052669,-0.03984161,0.064883254,0.024450615,0.041991018,0.017981533,0.041077986,0.063589826,-0.01104544,0.084603734,0.04332131,0.0041385363,0.07286733,-0.018887782,0.076195724,-0.021489078,-0.045993663,-0.052787732,0.0075105582,-0.0023860163,-0.010520825,0.0021552118,-0.08051806,0.061299916,-0.06347632,0.009948235,-0.06928475,-0.027916241,-0.016164059,-0.053455036,0.050054267,0.028922388,-0.028242402,-0.024950111,0.026767386,0.016913066,0.027214397,-0.027960636,0.09856684,0.0005708672,-0.06328706,-0.044772126,0.07103153,-0.014054399,0.044642996,-0.008021074,-0.07419962,0.09611253,0.044166476,0.0086504435,0.016296905,0.0574119,-0.0057896064,-0.10163813,0.042229597,-1.7167856e-8,0.008097164,0.0021034488,-0.00032095044,-0.022964565,0.014754429,0.033331983,0.028695943,-0.06141144,-0.021539602,0.026180083,0.06848516,0.035678882,-0.03490754,0.03020872,0.016107157,0.032306205,0.06105696,-0.002890444,0.0042259926,-0.030309584,-0.05571212,-0.028685763,0.0010230955,-0.061070923,-0.015631136,-0.050229665,0.009892334,0.09850854,-0.011604715,-0.01747272,0.025676185,0.0343539,-0.037994694,-0.10855406,0.0031102486,0.009522714,-0.036329266,0.032970943,0.051182617,-0.06983549,-0.06184144,0.039326083,0.032272592,0.044087004,0.06657253,0.04511283,0.081648186,0.02202941,-0.016903158,-0.070771135,-0.037015453,-0.0062212045,0.18698506,0.044452403,-0.0026717186,0.0044811214,0.010816393,0.014528357,-0.036223397,0.01895795,-0.064832956,0.02188936,0.049300164,0.033600762 }).Select(d => (float)d).ToArray()
                },
            };
            var dogQueryVector = (new double[] { -0.053202216, 0.01422119, 0.007062546, 0.0685742, -0.07858203, 0.010138983, 0.10238025, -0.012096751, 0.09522599, -0.030270875, 0.002181861, -0.064782545, -0.0026875706, 0.0060957014, -0.003964779, -0.030604681, -0.047901124, -0.019261848, -0.059947517, -0.10413115, -0.08611966, 0.03632282, -0.025586247, 0.0017129881, -0.07146128, 0.061734077, 0.017160414, -0.05659205, 0.0248427, -0.07782747, -0.032485314, -0.008684083, -0.011535832, 0.038153064, -0.057013486, -0.053252906, 0.004985692, 0.032392446, 0.0725966, 0.032940567, 0.024707653, -0.083363794, -0.015673108, -0.04811024, -0.003449794, 0.004415103, -0.035913676, -0.051946636, 0.015592655, 0.0035385543, -0.010283442, 0.047748506, -0.040175628, -0.009133693, -0.03460812, -0.03693011, -0.04091714, 0.0176677, -0.00934914, -0.053623937, 0.011154383, 0.016148455, 0.013840816, 0.028249927, 0.04024405, 0.02096661, -0.014487404, -0.0016292258, -0.004891051, 0.012042645, 0.04556029, 0.0130860545, 0.070578784, -0.03086842, 0.030368855, -0.10848343, 0.05554082, -0.017487692, 0.16430159, 0.051410932, -0.027641848, -0.029989198, -0.057063058, 0.056793693, 0.050923523, 0.015136637, -0.0012497514, 0.02384801, -0.06327192, 0.028891006, -0.055418354, -0.03496716, 0.03029518, 0.026919777, -0.08353811, 0.018368296, -0.03516996, -0.08284338, -0.07195326, 0.19801475, 0.016410688, 0.0445346, -0.003741409, -0.038506165, 0.053398475, -0.0034389244, -0.04352991, 0.06336845, -0.013076868, -0.019743098, -0.045236666, 0.020782078, -0.056481004, 0.057446502, 0.055468243, 0.021229729, -0.100917056, -0.03422642, 0.02944804, -0.03325292, 0.028943142, 0.030092051, -0.051856354, 0.008190983, -0.016726157, -0.08435183, 0.011159818, -5.9255234e-33, 0.030620761, -0.085034214, 0.0028181712, -0.041073505, -0.042798948, 0.041067425, 0.029467635, 0.036486518, -0.12122617, 0.013526328, -0.01391842, 0.0312512, -0.021689802, 0.01621624, 0.11224023, -0.006686669, -0.0018879274, 0.05318519, 0.03250415, -0.03782473, -0.046973582, 0.061971873, 0.063630275, 0.050121382, -0.007621213, -0.021432782, -0.03779708, -0.08284233, -0.026234223, 0.036130365, 0.041241154, 0.014499247, 0.073483825, 0.00073006714, -0.081418164, -0.055791657, -0.04209736, -0.096603446, -0.040196676, 0.028519753, 0.12910499, 0.010470544, 0.025057316, 0.01734334, -0.02719573, -0.0049704155, 0.015811851, 0.03439927, -0.044550493, 0.020814221, 0.027571082, -0.014297911, 0.028702551, -0.021064728, 0.008865078, 0.009936881, 0.0029201612, -0.023835903, 0.012977942, 0.06633931, 0.068944834, 0.082585804, 0.008766892, -0.013999867, 0.09115506, -0.122037254, -0.045294352, -0.018009886, -0.022158505, 0.02152304, -0.03885241, -0.019468945, 0.07964807, -0.015691828, 0.06885623, -0.015452343, 0.022757484, 0.025256434, -0.03119467, -0.033447854, -0.021564618, -0.010073421, 0.0055514527, 0.048961196, -0.021559088, 0.06377866, -0.019740583, -0.030324804, 0.0062891715, 0.045206502, -0.045785706, -0.049080465, 0.087099895, 0.027371299, 0.09064848, 3.433169e-33, 0.06266184, 0.028918529, 0.000108557906, 0.09145542, -0.030282516, 0.0048763165, -0.02540525, 0.066567004, -0.034166507, 0.047780972, -0.03424499, 0.007805756, 0.10785121, 0.008996277, 0.0076608267, 0.08868162, 0.0036972803, -0.030516094, 0.02168669, -0.004358315, -0.14477515, 0.011545589, 0.018421879, -0.025913069, -0.05191015, 0.03943329, 0.037553225, -0.0147632975, -0.022263186, -0.048638437, -0.0065658195, -0.039633695, -0.041322067, -0.02844163, 0.010661134, 0.15864708, 0.04770698, -0.04730114, -0.06286664, 0.008440104, 0.059898064, 0.019403962, -0.03227739, 0.11167067, 0.016108502, 0.052688885, -0.017888643, -0.0058668335, 0.052891612, 0.018419184, -0.04730259, -0.014312523, 0.030081172, -0.07333967, -0.012648647, 0.004494484, -0.09500656, 0.018896673, -0.029087285, -0.0051991083, -0.0029317876, 0.069698535, 0.012463835, 0.1219864, -0.10485225, -0.05362739, -0.0128166545, -0.027964052, 0.05004069, -0.07638481, 0.024308309, 0.04531832, -0.029027926, 0.010168302, -0.010628256, 0.030930692, -0.046634875, 0.0045742486, 0.007714686, -0.0063424213, -0.07790265, -0.06532262, -0.047622908, 0.010272605, -0.056622025, -0.011285954, 0.0020759962, 0.06382898, -0.013343911, -0.03008575, -0.009862737, 0.054995734, -0.021704284, -0.05336612, -0.02860762, -1.3317537e-8, -0.028604865, -0.029213138, -0.04298399, -0.019619852, 0.09963344, 0.0694588, -0.030038442, -0.0401437, -0.006644881, 0.026138376, 0.044374008, -0.01637589, -0.06998592, 0.013482148, 0.04653866, -0.0153024765, -0.053351574, 0.039734483, 0.06283631, 0.07712063, -0.050968867, 0.03027798, 0.055424906, 0.0023063482, -0.051206734, -0.035924364, 0.04564326, 0.106056266, -0.08215607, 0.038128633, -0.022592563, 0.14054875, -0.07613521, -0.03006324, -0.0040755956, -0.06966433, 0.07610892, -0.07929878, 0.024970463, 0.03414342, 0.050462823, 0.15209967, -0.020093411, -0.079005316, -0.0006247459, 0.062248245, 0.026453331, -0.12163222, -0.028260367, -0.056446116, -0.09818232, -0.0074948515, 0.027907023, 0.06908376, 0.014955464, 0.005030419, -0.0131421015, -0.047915705, -0.01678274, 0.03665314, 0.1114189, 0.029845735, 0.02391984, 0.110152245 }).Select(d => (float)d).ToArray();

            var options = new CollectionDefinition
            {
                Vector = new VectorOptions
                {
                    Dimension = 384
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithVector>(collectionName, options);
            var insertResult = await collection.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var result = collection.Find(new FindOptions<SimpleObjectWithVector>() { Sort = Builders<SimpleObjectWithVector>.Sort.Vector(dogQueryVector) }, null);
            Assert.Equal("This is about a dog.", result.First().Name);

        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task QueryDocumentsWithVectorizeAsync()
    {
        var collectionName = "simpleObjectsWithVectorize";
        try
        {
            List<SimpleObjectWithVectorize> items = new List<SimpleObjectWithVectorize>() {
                new()
                {
                    Id = 0,
                    Name = "This is about a cat.",
                },
                new()
                {
                    Id = 1,
                    Name = "This is about a dog.",
                },
                new()
                {
                    Id = 2,
                    Name = "This is about a horse.",
                },
            };
            var dogQueryVectorString = "dog";

            var options = new CollectionDefinition
            {
                Vector = new VectorOptions
                {
                    Metric = SimilarityMetric.Cosine,
                    Service = new VectorServiceOptions
                    {
                        Provider = "nvidia",
                        ModelName = "NV-Embed-QA"
                    }
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithVectorize>(collectionName, options);
            var insertResult = await collection.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var finder = collection.Find<SimpleObjectWithVectorizeResult>(new FindOptions<SimpleObjectWithVectorize>() { Sort = Builders<SimpleObjectWithVectorize>.Sort.Vectorize(dogQueryVectorString), IncludeSimilarity = true, IncludeSortVector = true }, null);
            var cursor = finder.ToCursor();
            var list = cursor.ToList();
            var result = list.First();
            Assert.Equal("This is about a dog.", result.Name);
            Assert.NotNull(result.Similarity);
            Assert.NotNull(cursor.SortVectors);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public async Task QueryDocumentsWithVectorize_Fluent_Async()
    {
        var collectionName = "simpleObjectsWithVectorizeFluent";
        try
        {
            List<SimpleObjectWithVectorize> items = new List<SimpleObjectWithVectorize>() {
                new()
                {
                    Id = 0,
                    Name = "This is about a cat.",
                },
                new()
                {
                    Id = 1,
                    Name = "This is about a dog.",
                },
                new()
                {
                    Id = 2,
                    Name = "This is about a horse.",
                },
            };
            var dogQueryVectorString = "dog";

            var options = new CollectionDefinition
            {
                Vector = new VectorOptions
                {
                    Metric = SimilarityMetric.Cosine,
                    Service = new VectorServiceOptions
                    {
                        Provider = "nvidia",
                        ModelName = "NV-Embed-QA"
                    }
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SimpleObjectWithVectorize>(collectionName, options);
            var insertResult = await collection.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var finder = collection.Find<SimpleObjectWithVectorizeResult>().Sort(
                Builders<SimpleObjectWithVectorize>.Sort.Vectorize(dogQueryVectorString)).IncludeSimilarity(true).IncludeSortVector(true);
            var cursor = finder.ToCursor();
            var list = cursor.ToList();
            var result = list.First();
            Assert.Equal("This is about a dog.", result.Name);
            Assert.NotNull(result.Similarity);
            Assert.NotNull(cursor.SortVectors);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collectionName);
        }
    }

    [Fact]
    public void Distinct_TopLevel()
    {
        var collection = fixture.SearchCollection;
        var distinct = collection.Find().DistinctBy(so => so.Name);
        Assert.Equal(33, distinct.Count());
    }

    [Fact]
    public void Distinct_Nested()
    {
        var collection = fixture.SearchCollection;
        var distinct = collection.Find().DistinctBy(so => so.Properties.PropertyOne);
        Assert.Equal(4, distinct.Count());
    }

    [Fact]
    public void Distinct_WithFilter()
    {
        var collection = fixture.SearchCollection;
        var builder = Builders<SimpleObject>.Filter;
        var filter = builder.Lt(so => so.Properties.IntProperty, 20);
        var distinct = collection.Find(filter).DistinctBy(so => so.Properties.PropertyOne);
        Assert.Equal(3, distinct.Count());
    }
}

