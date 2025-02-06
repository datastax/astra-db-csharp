using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Collections;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Tests;

[CollectionDefinition("Admin Collection")]
public class AdminCollection : ICollectionFixture<AdminFixture>
{

}

[Collection("Admin Collection")]
public class AdminTests
{
	AdminFixture fixture;

	public AdminTests(AdminFixture fixture)
	{
		this.fixture = fixture;
	}

	[Fact]
	public async Task ConnectViaDbId()
	{
		var dbId = fixture.DatabaseId;
		var database = await fixture.Client.GetDatabaseAsync(dbId);
		Assert.NotNull(database);

		database = fixture.Client.GetDatabase(dbId);
		Assert.NotNull(database);
	}

	[Fact]
	public async Task GetDatabasesList()
	{
		var list = await fixture.Client.GetAstraAdmin().ListDatabasesAsync();
		Assert.NotNull(list);

		list = fixture.Client.GetAstraAdmin().ListDatabases();
		Assert.NotNull(list);

		Console.WriteLine($"GetDatabasesList: {list.Count} items");
	}

	[Fact]
	public async Task GetDatabasesNamesList()
	{
		var list = await fixture.Client.GetAstraAdmin().ListDatabaseNamesAsync();
		Assert.NotNull(list);

		list = fixture.Client.GetAstraAdmin().ListDatabaseNames();
		Assert.NotNull(list);

		Console.WriteLine($"GetDatabasesNamesList: {list.Count} items");
		Console.WriteLine(string.Join(", ", list));
	}

	[Fact]
	public async Task CheckDatabaseExistsByName()
	{
		var dbName = "test-1";

		var found = await fixture.Client.GetAstraAdmin().DoesDatabaseExistAsync(dbName);
		Assert.True(found);

		found = fixture.Client.GetAstraAdmin().DoesDatabaseExist(dbName);
		Assert.True(found);
	}

	[Theory]
	[InlineData(null)]
	[InlineData("")]
	public void CheckDatabaseExistsByName_ExpectedError(string invalidName)
	{
		var ex = Assert.Throws<ArgumentException>(() => fixture.Client.GetAstraAdmin().DoesDatabaseExist(invalidName));
		Assert.Contains("Value cannot be null or empty", ex.Message);
	}

	[Theory]
	[InlineData(null)]
	[InlineData("")]
	public async Task CheckDatabaseExistsByNameAsync_ExpectedError(string invalidName)
	{
		var ex = await Assert.ThrowsAsync<ArgumentException>(
			() => fixture.Client.GetAstraAdmin().DoesDatabaseExistAsync(invalidName)
		);
		Assert.Contains("Value cannot be null or empty", ex.Message);
	}

	[Fact]
	public async Task CheckDatabaseExistsByName_ExpectedFalse()
	{
		var dbName = "this-is-not-the-greatest-db-in-the-world-this-is-a-tribute";

		var found = await fixture.Client.GetAstraAdmin().DoesDatabaseExistAsync(dbName);
		Assert.False(found);

		found = fixture.Client.GetAstraAdmin().DoesDatabaseExist(dbName);
		Assert.False(found);
	}

	[Fact]
	public async Task CheckDatabaseExistsById()
	{
		// todo: get this value from an expected named DB produced by testing CreateDatabase()
		var dbId = fixture.DatabaseId;

		var found = await fixture.Client.GetAstraAdmin().DoesDatabaseExistAsync(dbId);
		Assert.True(found);

		found = fixture.Client.GetAstraAdmin().DoesDatabaseExist(dbId);
		Assert.True(found);
	}
}
