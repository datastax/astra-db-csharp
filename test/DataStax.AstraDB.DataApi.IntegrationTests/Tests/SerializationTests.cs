using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
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
				DateTimeProperty = DateTime.Now,
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
				DateTimeProperty = DateTime.Now,
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
		string serializationTestString = "{\"_id\":19,\"Name\":\"Animal19\",\"Properties\":{\"PropertyOne\":\"groupthree\",\"PropertyTwo\":\"animal19\",\"IntProperty\":20,\"StringArrayProperty\":[\"animal19\",\"animal119\",\"animal219\"],\"BoolProperty\":true,\"DateTimeProperty\":\"2019-05-19T00:00:00\",\"DateTimeOffsetProperty\":\"0001-01-01T00:00:00+00:00\"}}";
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
		var command = new Command("deserializationTest", new DataApiClient(), commandOptions, null);
		var deserialized = command.Deserialize<TableInsertManyResult>(serializationTestString);
		Assert.Equal("text", deserialized.PrimaryKeys["Name"].Type);
		Assert.Equal("Test", deserialized.InsertedIds.First().First().ToString());
	}

	[Fact]
	public void BookDeserializationTest()
	{
		var serializationTestString = "{\"_id\":\"3a0cdac3-679b-435a-8cda-c3679bf35a6b\",\"title\":\"Test Book 1\",\"author\":\"Test Author 1\",\"number_of_pages\":100}";
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
		var serializationTestString = "{\"data\":{\"documents\":[{\"_id\":\"f9bf6e20-6efd-421d-bf6e-206efdb21d49\",\"Name\":\"Cat\"},{\"_id\":\"a02bb811-98e6-416f-abb8-1198e6816fa5\",\"Name\":\"Cat\"},{\"_id\":\"03479720-4ba5-4928-8797-204ba5392893\",\"Name\":\"NotCat\"},{\"_id\":\"54726b36-8202-4f72-b26b-368202ef7209\",\"Name\":\"NotCat\"},{\"_id\":\"6d8ea948-7ac1-49f6-8ea9-487ac149f6d7\",\"Name\":\"Cow\"},{\"_id\":\"aeee0998-9941-4015-ae09-989941e0158d\",\"Name\":\"Cow\"},{\"_id\":\"817fbc98-13d0-4aca-bfbc-9813d0dacae9\",\"Name\":\"Horse\"},{\"_id\":\"8be82ac3-ff2c-4f2b-a82a-c3ff2cbf2bd7\",\"Name\":\"Horse\"}],\"nextPageState\":null},\"status\":{\"documentResponses\":[{\"scores\":{\"$rerank\":1.7070312,\"$vector\":0.75348675,\"$vectorRank\":1,\"$bm25Rank\":1,\"$rrf\":0.032786883}},{\"scores\":{\"$rerank\":1.7070312,\"$vector\":0.75348675,\"$vectorRank\":2,\"$bm25Rank\":2,\"$rrf\":0.032258064}},{\"scores\":{\"$rerank\":1.2802734,\"$vector\":0.71900904,\"$vectorRank\":5,\"$bm25Rank\":3,\"$rrf\":0.031257633}},{\"scores\":{\"$rerank\":1.2802734,\"$vector\":0.71900904,\"$vectorRank\":6,\"$bm25Rank\":4,\"$rrf\":0.030776516}},{\"scores\":{\"$rerank\":-3.4140625,\"$vector\":0.741625,\"$vectorRank\":3,\"$bm25Rank\":5,\"$rrf\":0.031257633}},{\"scores\":{\"$rerank\":-3.4140625,\"$vector\":0.741625,\"$vectorRank\":4,\"$bm25Rank\":6,\"$rrf\":0.030776516}},{\"scores\":{\"$rerank\":-9.1015625,\"$vector\":0.69422597,\"$vectorRank\":7,\"$bm25Rank\":null,\"$rrf\":0.014925373}},{\"scores\":{\"$rerank\":-9.1015625,\"$vector\":0.69422597,\"$vectorRank\":8,\"$bm25Rank\":null,\"$rrf\":0.014705882}}]}}";
		var commandOptions = new List<CommandOptions>
		{
			new CommandOptions()
			{
				OutputConverter = new DocumentConverter<HybridSearchTestObject>()
			}
		};
		var command = new Command("deserializationTest", new DataApiClient(), commandOptions.ToArray(), null);
		var deserialized = command.Deserialize<ApiResponseWithData<ApiFindResult<HybridSearchTestObject>, FindStatusResult<RerankedResult<HybridSearchTestObject>>>>(serializationTestString);
		Assert.NotNull(deserialized);
		Assert.NotNull(deserialized.Data);
		Assert.NotNull(deserialized.Status);
		Assert.NotNull(deserialized.Status.DocumentResponses);
		Assert.NotEmpty(deserialized.Status.DocumentResponses.First().Scores);
	}

}
