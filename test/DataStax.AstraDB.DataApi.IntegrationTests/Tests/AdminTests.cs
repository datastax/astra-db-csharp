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
}
