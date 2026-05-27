using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Enumeration;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.SerDes;
using DataStax.AstraDB.DataApi.Tables;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Admin Collection")]
public class SerializationTests
{
    AdminFixture fixture;

    public SerializationTests(AssemblyFixture assemblyFixture, AdminFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public void TestTypeSerializationDeserialization()
    {
        var collection = fixture.Database.GetCollection<SerializationTest>("serializationTest");
        var testObject = new SerializationTest()
        {
            TestId = 1,
            NestedProperties = new Properties()
            {
                PropertyOne = "One",
                PropertyTwo = "Two",
                IntProperty = 1,
                StringArrayProperty = new string[] { "One", "Two", "Three" },
                BoolProperty = true,
                DateTimeProperty = DateTime.UtcNow,
                DateTimeOffsetProperty = DateTimeOffset.Now,
                SkipWhenNull = null
            }
        };
        string serialized = collection.CheckSerialization(testObject);
        var deserialized = collection.CheckDeserialization(serialized);
        Assert.Equal(testObject.TestId, deserialized.TestId);
        Assert.Equal(testObject.NestedProperties.BoolProperty, deserialized.NestedProperties.BoolProperty);
        Assert.Equal(testObject.NestedProperties.PropertyOne, deserialized.NestedProperties.PropertyOne);
        Assert.Equal(testObject.NestedProperties.PropertyTwo, deserialized.NestedProperties.PropertyTwo);
        Assert.Equal(testObject.NestedProperties.IntProperty, deserialized.NestedProperties.IntProperty);
        Assert.Equal(testObject.NestedProperties.StringArrayProperty, deserialized.NestedProperties.StringArrayProperty);
        Assert.Equal(testObject.NestedProperties.DateTimeOffsetProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"), deserialized.NestedProperties.DateTimeOffsetProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"));
        Assert.Equal(testObject.NestedProperties.DateTimeProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"), deserialized.NestedProperties.DateTimeProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"));
        Assert.Equal(testObject.NestedProperties.SkipWhenNull, deserialized.NestedProperties.SkipWhenNull);
    }

    [Fact]
    public void TestTypeSerializationDeserialization_WithDocumentSerializer()
    {
        var collection = fixture.Database.GetCollection<SerializationTest>("serializationTest");
        var testObject = new SerializationTest()
        {
            TestId = 1,
            NestedProperties = new Properties()
            {
                PropertyOne = "One",
                PropertyTwo = "Two",
                IntProperty = 1,
                StringArrayProperty = new string[] { "One", "Two", "Three" },
                BoolProperty = true,
                DateTimeProperty = DateTime.UtcNow,
                DateTimeOffsetProperty = DateTimeOffset.Now,
                SkipWhenNull = null
            }
        };
        var commandOptions = new CommandOptions();
        commandOptions.SetConvertersIfNull(new DocumentConverter<SerializationTest>(), new DocumentConverter<SerializationTest>());
        string serialized = collection.CheckSerialization(testObject, commandOptions);
        var deserialized = collection.CheckDeserialization(serialized, commandOptions);
        Assert.Equal(testObject.TestId, deserialized.TestId);
        Assert.Equal(testObject.NestedProperties.BoolProperty, deserialized.NestedProperties.BoolProperty);
        Assert.Equal(testObject.NestedProperties.PropertyOne, deserialized.NestedProperties.PropertyOne);
        Assert.Equal(testObject.NestedProperties.PropertyTwo, deserialized.NestedProperties.PropertyTwo);
        Assert.Equal(testObject.NestedProperties.IntProperty, deserialized.NestedProperties.IntProperty);
        Assert.Equal(testObject.NestedProperties.StringArrayProperty, deserialized.NestedProperties.StringArrayProperty);
        Assert.Equal(testObject.NestedProperties.DateTimeOffsetProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"), deserialized.NestedProperties.DateTimeOffsetProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"));
        Assert.Equal(testObject.NestedProperties.DateTimeProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"), deserialized.NestedProperties.DateTimeProperty.ToUniversalTime().ToString("yyyyMMddHHmmss"));
        Assert.Equal(testObject.NestedProperties.SkipWhenNull, deserialized.NestedProperties.SkipWhenNull);
    }

    [Fact]
    public void TestSpecific()
    {
        string serializationTestString = "{\"_id\":19,\"Name\":\"Animal19\",\"Properties\":{\"PropertyOne\":\"groupthree\",\"PropertyTwo\":\"animal19\",\"IntProperty\":20,\"StringArrayProperty\":[\"animal19\",\"animal119\",\"animal219\"],\"BoolProperty\":true,\"DateTimeProperty\":\"2019-05-19T00:00:00\",\"DateTimeOffsetProperty\":\"2001-01-01T00:00:00+00:00\"}}";
        var collection = fixture.Database.GetCollection<SimpleObject>("serializationTest2");
        var commandOptions = new CommandOptions()
        {
            OutputConverter = new DocumentConverter<SimpleObject>()
        };
        var deserialized = collection.CheckDeserialization(serializationTestString, commandOptions);
    }

    [Fact]
    public void IdList_Guid()
    {
        string serializationTestString = "{\"insertedIds\":[{\"$uuid\":\"315c2015-e404-432c-9c20-15e404532ceb\"}]}";
        var collection = fixture.Database.GetCollection<CollectionInsertManyResult<object>>("serializationTest");
        var commandOptions = new CommandOptions()
        {
            OutputConverter = new IdListConverter()
        };
        var deserialized = collection.CheckDeserialization(serializationTestString, commandOptions);
    }

    [Fact]
    public void IdList_ObjectId()
    {
        string serializationTestString = "{\"insertedIds\":[{\"$objectId\":\"67eaab273cc8411120638d65\"}]}";
        var collection = fixture.Database.GetCollection<CollectionInsertManyResult<object>>("serializationTest");
        var commandOptions = new CommandOptions()
        {
            OutputConverter = new IdListConverter()
        };
        var deserialized = collection.CheckDeserialization(serializationTestString, commandOptions);
    }

    [Fact]
    public void TableInsertManyResult()
    {
        string serializationTestString = "{\"primaryKeySchema\":{\"Name\":{\"type\":\"text\"}},\"insertedIds\":[[\"Test\"]]}";
        var commandOptions = Array.Empty<CommandOptions>();
        var command = new Command("deserializationTest", new DataAPIClient(), commandOptions, null);
        var deserialized = command.Deserialize<TableInsertManyResult>(serializationTestString);
        Assert.Equal("text", deserialized.PrimaryKeys["Name"].Type);
        Assert.Equal("Test", deserialized.InsertedIdTuples.First().First().ToString());
    }

    [Fact]
    public void BookDeserializationTest()
    {
        var serializationTestString = "{\"data\",{\"documents\",[{\"_id\",\"11111111-1111-1111-1111-111111111111\";\"Name\",\"One\"};{\"_id\",\"22222222-2222-2222-2222-222222222222\";\"Name\",\"Two\"}];\"nextPageState\",null};\"status\",{\"documentResponses\",[{\"scores\",{\"$rerank\",1.0;\"$vector\",1.0;\"$vectorRank\",1;\"$bm25Rank\",1;\"$rrf\",1.0}};{\"scores\",{\"$rerank\",2.0;\"$vector\",2.0;\"$vectorRank\",2;\"$bm25Rank\",2;\"$rrf\",null}}]}}";
        var collection = fixture.Database.GetCollection<Book>("bookTestTable");
        var commandOptions = new CommandOptions()
        {
            OutputConverter = new DocumentConverter<Book>()
        };
        var deserialized = collection.CheckDeserialization(serializationTestString, commandOptions);
        Assert.Equal("Test Book 1", deserialized.Title);
        Assert.Equal("Test Author 1", deserialized.Author);
        Assert.Equal(100, deserialized.NumberOfPages);
    }

    [Fact]
    public void HybridSearchResponseDeserializationTest()
    {
        var serializationTestString = "{\"data\":{\"documents\":[{\"_id\":111,\"Name\":\"One\"},{\"_id\":222,\"Name\":\"Two\"}],\"nextPageState\":null},\"status\":{\"documentResponses\":[{\"scores\":{\"$rerank\":1.0,\"$vector\":1.0,\"$vectorRank\":1,\"$bm25Rank\":1,\"$rrf\":1.0}},{\"scores\":{\"$rerank\":2.0,\"$vector\":2.0,\"$vectorRank\":2,\"$bm25Rank\":2,\"$rrf\":null}}]}}";
        var commandOptions = new List<CommandOptions>
        {
            new CommandOptions()
            {
                OutputConverter = new DocumentConverter<SimpleObjectWithVector>()
            }
        };
        var command = new Command("deserializationTest", new DataAPIClient(), commandOptions.ToArray(), null);
        var deserialized = command.Deserialize<APIResponseWithData<APIFindResult<SimpleObjectWithVector>, APIFindAndRerankStatusResults>>(serializationTestString);
        Assert.NotNull(deserialized);
        Assert.NotNull(deserialized.Data);
        Assert.NotNull(deserialized.Status);
        Assert.NotNull(deserialized.Status.DocumentResponses);
        Assert.NotEmpty(deserialized.Status.DocumentResponses.First().Scores);
    }

    [Fact]
    public void TimeUuid_TypedTest()
    {
        var serializationTestString = @"
            {
                ""data"": {
                    ""document"": {
                        ""tuid"": ""a448ba80-1723-11f1-aedc-e7a263c8acfc"",
                        ""id"": ""the_row""
                    }
                },
                ""status"": {
                    ""projectionSchema"": {
                        ""id"": {
                            ""type"": ""text""
                        },
                        ""tuid"": {
                            ""type"": ""timeuuid"",
                            ""apiSupport"": {
                                ""createTable"": false,
                                ""insert"": true,
                                ""read"": true,
                                ""filter"": true,
                                ""cqlDefinition"": ""timeuuid""
                            }
                        }
                    }
                }
            }
        ";
        var commandOptions = new List<CommandOptions>
        {
            new CommandOptions()
            {
                OutputConverter = new DocumentConverter<TimeUuidObject>()
            }
        };
        var command = new Command("deserializationTest", new DataAPIClient(), commandOptions.ToArray(), null);

        var deserialized = command.Deserialize<APIResponseWithData<DocumentResult<TimeUuidObject>, TableFindStatusResult>>(serializationTestString);
        Assert.NotNull(deserialized);
        Assert.NotNull(deserialized.Data);
        Assert.NotNull(deserialized.Data.Document);
        Assert.Equal(TimeUuid.Parse("a448ba80-1723-11f1-aedc-e7a263c8acfc"), deserialized.Data.Document.tuid);
    }

    [Fact]
    public void Test_TimeOnly()
    {
        var serializationTestString = @"
            {""TimestampWithKind"":""2026-03-26T21:30:03.269Z"",""Duration"":""P12Y3M1DT12H30M5.012007002S"",""Time"":""22:30:03.269601500"",""String"":""Test 3"",""Double"":1.7976931348623157E308,""Timestamp"":""2026-03-26T21:30:03.269Z"",""Int"":2147483647,""Date"":""2026-03-26"",""TinyInt"":255,""Float"":3.4028235E38,""Decimal"":79228162514264337593543950335,""MaybeDate"":""2026-03-26"",""BigInt"":9223372036854775807,""SmallInt"":32767,""Boolean"":false,""UUID"":""74cf37ac-db3f-49f5-abd2-3dcbf0add03c""}
        ";
        var collection = fixture.Database.GetCollection<TypesTester>("serializationTest2");
        var commandOptions = new CommandOptions()
        {
            OutputConverter = new DocumentConverter<TypesTester>()
        };
        var deserialized = collection.CheckDeserialization(serializationTestString, commandOptions);
        Assert.Equal(TimeOnly.Parse("22:30:03.2696015"), deserialized.Time);
    }

    [Fact]
    public void Test_BinaryVector()
    {
        var serializationTestString = @"
            {""_id"":""1f7478e1-fc54-4d76-b478-e1fc54dd767d"",""$vector"":{""$binary"":""PczMzb5MzM0+mZma""}}
        ";
        var collection = fixture.Database.GetCollection<BinaryVectorObject>("serializationTest3");
        var commandOptions = new CommandOptions()
        {
            OutputConverter = new DocumentConverter<BinaryVectorObject>()
        };
        var deserialized = collection.CheckDeserialization(serializationTestString, commandOptions);
        Assert.NotNull(deserialized.TheVector);
    }

    private static readonly float[] _knownBinaryFloats = new[] { 0.1f, -0.2f, 0.3f };
    private const string _knownBinaryBase64 = "PczMzb5MzM0+mZma";

    [Fact]
    public void Test_PlainFloatArray_Serialization_ProducesJsonArray()
    {
        var obj = new PlainFloatArrayObject { _id = "test-pfa-1", Values = new[] { 1.0f, 2.0f, 3.0f } };
        var collection = fixture.Database.GetCollection<PlainFloatArrayObject>("serializationTest3");
        string serialized = collection.CheckSerialization(obj);
        Assert.Contains("\"Values\": [", serialized);
        Assert.DoesNotContain("$binary", serialized);
    }

    [Fact]
    public void Test_DocumentMappingVector_Serialization_ProducesBinaryInsideDollarVector()
    {
        var obj = new BinaryVectorObject { _id = "test-dmv-1", TheVector = new[] { 0.1f, -0.2f, 0.3f } };
        var collection = fixture.Database.GetCollection<BinaryVectorObject>("serializationTest3");
        var commandOptions = new CommandOptions() { InputConverter = new DocumentConverter<BinaryVectorObject>() };
        string serialized = collection.CheckSerialization(obj, commandOptions);
        Assert.Contains("\"$vector\":", serialized);
        Assert.Contains("\"$binary\":", serialized);
    }

    [Fact]
    public void Test_RowConverter_ColumnVector_Serialization_ProducesBinary()
    {
        var row = new RowTestObject { Name = "test-row-1", Vector = new[] { 0.1f, 0.2f, 0.3f, 0.4f } };
        var commandOptions = new[] { new CommandOptions() { InputConverter = new RowConverter<RowTestObject>() } };
        var command = new Command("serializationTest", new DataAPIClient(), commandOptions, null);
        string serialized = command.Serialize(row);
        Assert.Contains("\"$binary\":", serialized);
        Assert.DoesNotContain("\"Vector\": [", serialized);
    }

    [Fact]
    public void Test_FloatArrayWriter_Serialization_ProducesJsonArray()
    {
        var obj = new FloatArrayWriterObject { _id = "test-fa-1", Vector = new[] { 1.0f, 2.0f, 3.0f } };
        var collection = fixture.Database.GetCollection<FloatArrayWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatArrayWriterObject>() };
        string serialized = collection.CheckSerialization(obj, commandOptions);
        Assert.Contains("\"Vector\": [", serialized);
        Assert.DoesNotContain("$binary", serialized);
    }

    [Fact]
    public void Test_FloatBinaryWriter_Serialization_ProducesBinaryFormat()
    {
        var obj = new FloatBinaryWriterObject { _id = "test-fb-1", Vector = new[] { 1.0f, 2.0f, 3.0f } };
        var collection = fixture.Database.GetCollection<FloatBinaryWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatBinaryWriterObject>() };
        string serialized = collection.CheckSerialization(obj, commandOptions);
        Assert.Contains("\"$binary\":", serialized);
        Assert.DoesNotContain("[", serialized[serialized.IndexOf("Vector")..]);
    }

    [Fact]
    public void Test_FloatArrayWriter_Deserialization_FromJsonArray()
    {
        var json = @"{""_id"":""test-fa-2"",""Vector"":[0.1,0.2,0.3]}";
        var collection = fixture.Database.GetCollection<FloatArrayWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatArrayWriterObject>() };
        var deserialized = collection.CheckDeserialization(json, commandOptions);
        Assert.NotNull(deserialized.Vector);
        Assert.Equal(3, deserialized.Vector.Length);
        Assert.Equal(0.1f, deserialized.Vector[0]);
        Assert.Equal(0.2f, deserialized.Vector[1]);
        Assert.Equal(0.3f, deserialized.Vector[2]);
    }

    [Fact]
    public void Test_FloatArrayWriter_Deserialization_FromBinaryFormat()
    {
        var json = $@"{{""_id"":""test-fa-3"",""Vector"":{{""$binary"":""{_knownBinaryBase64}""}}}}";
        var collection = fixture.Database.GetCollection<FloatArrayWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatArrayWriterObject>() };
        var deserialized = collection.CheckDeserialization(json, commandOptions);
        Assert.NotNull(deserialized.Vector);
        Assert.Equal(_knownBinaryFloats.Length, deserialized.Vector.Length);
        Assert.Equal(_knownBinaryFloats[0], deserialized.Vector[0]);
        Assert.Equal(_knownBinaryFloats[1], deserialized.Vector[1]);
        Assert.Equal(_knownBinaryFloats[2], deserialized.Vector[2]);
    }

    [Fact]
    public void Test_FloatBinaryWriter_Deserialization_FromBinaryFormat()
    {
        var json = $@"{{""_id"":""test-fb-2"",""Vector"":{{""$binary"":""{_knownBinaryBase64}""}}}}";
        var collection = fixture.Database.GetCollection<FloatBinaryWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatBinaryWriterObject>() };
        var deserialized = collection.CheckDeserialization(json, commandOptions);
        Assert.NotNull(deserialized.Vector);
        Assert.Equal(_knownBinaryFloats.Length, deserialized.Vector.Length);
        Assert.Equal(_knownBinaryFloats[0], deserialized.Vector[0]);
        Assert.Equal(_knownBinaryFloats[1], deserialized.Vector[1]);
        Assert.Equal(_knownBinaryFloats[2], deserialized.Vector[2]);
    }

    [Fact]
    public void Test_FloatBinaryWriter_Deserialization_FromJsonArray()
    {
        var json = @"{""_id"":""test-fb-3"",""Vector"":[0.1,0.2,0.3]}";
        var collection = fixture.Database.GetCollection<FloatBinaryWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatBinaryWriterObject>() };
        var deserialized = collection.CheckDeserialization(json, commandOptions);
        Assert.NotNull(deserialized.Vector);
        Assert.Equal(3, deserialized.Vector.Length);
        Assert.Equal(0.1f, deserialized.Vector[0]);
        Assert.Equal(0.2f, deserialized.Vector[1]);
        Assert.Equal(0.3f, deserialized.Vector[2]);
    }

    [Fact]
    public void Test_FloatArrayWriter_Roundtrip()
    {
        var original = new FloatArrayWriterObject { _id = "test-fa-rt", Vector = new[] { 1.5f, -2.5f, 3.5f } };
        var collection = fixture.Database.GetCollection<FloatArrayWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatArrayWriterObject>() };
        string serialized = collection.CheckSerialization(original, commandOptions);
        var deserialized = collection.CheckDeserialization(serialized, commandOptions);
        Assert.NotNull(deserialized.Vector);
        Assert.Equal(original.Vector, deserialized.Vector);
    }

    [Fact]
    public void Test_FloatBinaryWriter_Roundtrip()
    {
        var original = new FloatBinaryWriterObject { _id = "test-fb-rt", Vector = new[] { 1.5f, -2.5f, 3.5f } };
        var collection = fixture.Database.GetCollection<FloatBinaryWriterObject>("serializationTest3");
        var commandOptions = new CommandOptions() { OutputConverter = new DocumentConverter<FloatBinaryWriterObject>() };
        string serialized = collection.CheckSerialization(original, commandOptions);
        var deserialized = collection.CheckDeserialization(serialized, commandOptions);
        Assert.NotNull(deserialized.Vector);
        Assert.Equal(original.Vector, deserialized.Vector);
    }


}

public class TimeUuidObject
{
    public string id { get; set; }
    public TimeUuid tuid { get; set; }
};
