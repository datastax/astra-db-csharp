using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.SerDes;
using System.Runtime.CompilerServices;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Admin Collection")]
public class SerializationTests
{
	AdminFixture fixture;
	readonly Database _database;

	public SerializationTests(AdminFixture fixture)
	{
		this.fixture = fixture;
		_database = fixture.GetDatabase();
	}

	[Fact]
	public void TestTypeSerializationDeserialization()
	{
		var collection = _database.GetCollection<SerializationTest>("serializationTest");
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
		var collection = _database.GetCollection<SerializationTest>("serializationTest");
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
		var collection = _database.GetCollection<SimpleObject>("serializationTest2");
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
		var collection = _database.GetCollection<InsertDocumentsCommandResponse<object>>("serializationTest");
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
		var collection = _database.GetCollection<InsertDocumentsCommandResponse<object>>("serializationTest");
		var commandOptions = new CommandOptions()
		{
			OutputConverter = new IdListConverter()
		};
		var deserialized = collection.CheckDeserialization(serializationTestString, commandOptions);
	}

}
