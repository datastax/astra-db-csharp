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
}
