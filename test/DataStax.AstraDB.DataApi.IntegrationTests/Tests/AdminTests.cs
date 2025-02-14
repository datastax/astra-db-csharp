using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Tests;

[CollectionDefinition("Admin Collection")]
public class AdminCollection : ICollectionFixture<AdminFixture>
{
	public const string SkipMessage = "Please read 'How to run these skipped tests'";
}

//  dotnet test --filter "FullyQualifiedName~DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests"
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
		var dbGuid = fixture.DatabaseId;
		var db = await fixture.Client.GetDatabaseAsync(dbGuid);
		Assert.NotNull(db);

		db = fixture.Client.GetDatabase(dbGuid);
		Assert.NotNull(db);
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
		var dbName = fixture.DatabaseName;

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
		var ex = Assert.Throws<ArgumentNullException>(() => fixture.Client.GetAstraAdmin().DoesDatabaseExist(invalidName));
		Assert.Contains("Value cannot be null or empty", ex.Message);
	}

	[Theory]
	[InlineData(null)]
	[InlineData("")]
	public async Task CheckDatabaseExistsByNameAsync_ExpectedError(string invalidName)
	{
		var ex = await Assert.ThrowsAsync<ArgumentNullException>(
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
		var dbId = fixture.DatabaseId;

		var found = await fixture.Client.GetAstraAdmin().DoesDatabaseExistAsync(dbId);
		Assert.True(found);

		found = fixture.Client.GetAstraAdmin().DoesDatabaseExist(dbId);
		Assert.True(found);
	}

	[Fact]
	public async Task CheckDatabaseStatus()
	{
		var dbName = fixture.DatabaseName;

		var status = await fixture.Client.GetAstraAdmin().GetDatabaseStatusAsync(dbName);
		Assert.Equal("ACTIVE", status);

		status = fixture.Client.GetAstraAdmin().GetDatabaseStatus(dbName);
		Assert.Equal("ACTIVE", status);
	}

	/*
        From here on are ad hoc tests for creating and dropping databases. 
        You will likely need to adjust details to match your execution details.

        How to run these skipped tests:
        1. Comment the attribute with the skip.
        2. Add a [Fact] attribute.
        3. Run the associated command from the terminal.
    */

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseBlocking
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void CreateDatabaseBlocking()
	{
		var dbName = "test-db-create-blocking-x";
		var admin = fixture.Client.GetAstraAdmin().CreateDatabase(dbName);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseBlockingAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseBlockingAsync()
	{
		var dbName = "test-db-create-blocking-async-x";
		var admin = await fixture.Client.GetAstraAdmin().CreateDatabaseAsync(dbName);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabase
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void CreateDatabase()
	{
		var dbName = "test-db-create-1";
		var admin = fixture.Client.GetAstraAdmin().CreateDatabase(dbName, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseAsync()
	{
		var dbName = "test-db-create-async-1";
		var admin = await fixture.Client.GetAstraAdmin().CreateDatabaseAsync(dbName, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseByOptions
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void CreateDatabaseByOptions()
	{
		var dbName = "test-db-create-options-1";
		var options = new DatabaseCreationOptions();
		options.Name = dbName;
		var admin = fixture.Client.GetAstraAdmin().CreateDatabase(options, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseByOptionsAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseByOptionsAsync()
	{
		var dbName = "test-db-create-options-async-1";
		var options = new DatabaseCreationOptions();
		options.Name = dbName;
		var admin = await fixture.Client.GetAstraAdmin().CreateDatabaseAsync(options, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseByName
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void DropDatabaseByName()
	{
		var dbName = "test-db-drop-by-name";
		var dropped = fixture.Client.GetAstraAdmin().DropDatabase(dbName);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseByNameAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DropDatabaseByNameAsync()
	{
		var dbName = "test-db-drop-by-name-async";
		var dropped = await fixture.Client.GetAstraAdmin().DropDatabaseAsync(dbName);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseById
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void DropDatabaseById()
	{
		var dbGuid = Guid.Parse("73c7b474-2464-42f3-8338-ec10dbf693df"); // from a db created ad-hoc on astra's site
		var dropped = fixture.Client.GetAstraAdmin().DropDatabase(dbGuid);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseByIdAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DropDatabaseByIdAsync()
	{
		var dbGuid = Guid.Parse("65b4cdb5-2f21-4550-99ce-8c2570d18c1a"); // from a db created ad-hoc on astra's site
		var dropped = await fixture.Client.GetAstraAdmin().DropDatabaseAsync(dbGuid);

		Assert.True(dropped);
	}
}
