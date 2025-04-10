using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Results;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

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
		var list = await fixture.Client.GetAstraDatabasesAdmin().ListDatabasesAsync();
		Assert.NotNull(list);

		list = fixture.Client.GetAstraDatabasesAdmin().ListDatabases();
		Assert.NotNull(list);

		Console.WriteLine($"GetDatabasesList: {list.Count} items");
	}

	[Fact]
	public async Task GetDatabasesNamesList()
	{
		var list = await fixture.Client.GetAstraDatabasesAdmin().ListDatabaseNamesAsync();
		Assert.NotNull(list);

		list = fixture.Client.GetAstraDatabasesAdmin().ListDatabaseNames();
		Assert.NotNull(list);

		Console.WriteLine($"GetDatabasesNamesList: {list.Count} items");
		Console.WriteLine(string.Join(", ", list));
	}

	[Fact]
	public async Task CheckDatabaseExistsByName()
	{
		var dbName = fixture.DatabaseName;

		var found = await fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExistAsync(dbName);
		Assert.True(found);

		found = fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(dbName);
		Assert.True(found);
	}

	[Theory]
	[InlineData(null)]
	[InlineData("")]
	public void CheckDatabaseExistsByName_ExpectedError(string invalidName)
	{
		var ex = Assert.Throws<ArgumentNullException>(() => fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(invalidName));
		Assert.Contains("Value cannot be null or empty", ex.Message);
	}

	[Theory]
	[InlineData(null)]
	[InlineData("")]
	public async Task CheckDatabaseExistsByNameAsync_ExpectedError(string invalidName)
	{
		var ex = await Assert.ThrowsAsync<ArgumentNullException>(
			() => fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExistAsync(invalidName)
		);
		Assert.Contains("Value cannot be null or empty", ex.Message);
	}

	[Fact]
	public async Task CheckDatabaseExistsByName_ExpectedFalse()
	{
		var dbName = "this-is-not-the-greatest-db-in-the-world-this-is-a-tribute";

		var found = await fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExistAsync(dbName);
		Assert.False(found);

		found = fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(dbName);
		Assert.False(found);
	}

	[Fact]
	public async Task CheckDatabaseExistsById()
	{
		var dbId = fixture.DatabaseId;

		var found = await fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExistAsync(dbId);
		Assert.True(found);

		found = fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(dbId);
		Assert.True(found);
	}

	[Fact]
	public async Task CheckDatabaseStatus()
	{
		var dbName = fixture.DatabaseName;

		var status = await fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatusAsync(dbName);
		Assert.Equal("ACTIVE", status);

		status = fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatus(dbName);
		Assert.Equal("ACTIVE", status);
	}

	[Fact]
	public async Task DatabaseAdminAstra_GetDatabaseAdminAstra()
	{
		var database = await fixture.Client.GetDatabaseAsync(fixture.DatabaseId);
		var daa = fixture.CreateAdmin(database);

		Assert.IsType<DatabaseAdminAstra>(daa);
	}

	[Fact]
	public async Task DatabaseAdminAstra_GetDatabase()
	{
		var database = await fixture.Client.GetDatabaseAsync(fixture.DatabaseId);
		var daa = fixture.CreateAdmin(database);

		Assert.IsType<Database>(daa.GetDatabase());
	}

	[Fact]
	public async Task DatabaseAdminAstra_GetApiEndpoint()
	{
		var database = await fixture.Client.GetDatabaseAsync(fixture.DatabaseId);
		var daa = fixture.CreateAdmin(database);

		Assert.Equal(fixture.DatabaseId, AdminFixture.GetDatabaseIdFromUrl(daa.GetApiEndpoint()));
	}

	[Fact]
	public async Task DatabaseAdminAstra_GetKeyspacesList()
	{
		var database = await fixture.Client.GetDatabaseAsync(fixture.DatabaseId);
		var daa = fixture.CreateAdmin(database);

		var names = await daa.ListKeyspaceNamesAsync();
		Assert.NotNull(names);

		names = daa.ListKeyspaceNames();
		Assert.NotNull(names);

		var list = names.ToList();
		Console.WriteLine($"ListKeyspaces: {list.Count} items");
		list.ForEach(Console.WriteLine);
	}

	[Fact]
	public async Task DatabaseAdminAstra_DoesKeyspaceExist()
	{
		var database = await fixture.Client.GetDatabaseAsync(fixture.DatabaseId);
		var daa = fixture.CreateAdmin(database);

		var keyspaceExists = await daa.KeyspaceExistsAsync("default_keyspace");
		Assert.True(keyspaceExists);

		keyspaceExists = daa.KeyspaceExists("default_keyspace");
		Assert.True(keyspaceExists);
	}

	[Fact]
	public async Task DatabaseAdminAstra_DoesKeyspaceExist_Another()
	{
		var database = await fixture.Client.GetDatabaseAsync(fixture.DatabaseId);
		var daa = fixture.CreateAdmin(database);
		var keyspaceName = "another_keyspace";

		try
		{
			var keyspaceExists = await daa.KeyspaceExistsAsync(keyspaceName);
			if (!keyspaceExists)
			{
				await daa.CreateKeyspaceAsync(keyspaceName);
				Thread.Sleep(30 * 1000); //wait for keyspace to be created
				keyspaceExists = await daa.KeyspaceExistsAsync(keyspaceName);
			}
			Assert.True(keyspaceExists);
		}
		finally
		{
			await daa.DropKeyspaceAsync(keyspaceName);
		}
	}

	[Fact]
	public async Task DatabaseAdminAstra_FindEmbeddingProvidersAsync()
	{
		var adminOptions = new CommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.DatabaseId, fixture.Client, adminOptions);

		var result = await daa.FindEmbeddingProvidersAsync(adminOptions, runSynchronously: false);
		Assert.NotNull(result);
		if (result.EmbeddingProviders.Count == 0)
		{
			Console.WriteLine("No embedding providers returned.");
		}
		else
		{
			Assert.NotEmpty(result.EmbeddingProviders);
		}

		var providers = result.EmbeddingProviders;

		Assert.NotNull(providers);
		Assert.NotEmpty(providers);
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
		var admin = fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(dbName);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseBlockingAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseBlockingAsync()
	{
		var dbName = "test-db-create-blocking-async-x";
		var admin = await fixture.Client.GetAstraDatabasesAdmin().CreateDatabaseAsync(dbName);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabase
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void CreateDatabase()
	{
		var dbName = "test-db-create-x";
		var admin = fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(dbName, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseAsync()
	{
		var dbName = "test-db-create-async-x";
		var admin = await fixture.Client.GetAstraDatabasesAdmin().CreateDatabaseAsync(dbName, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseByOptions
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void CreateDatabaseByOptions()
	{
		var dbName = "test-db-create-options-x";
		var options = new DatabaseCreationOptions();
		options.Name = dbName;
		var admin = fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(options, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.CreateDatabaseByOptionsAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseByOptionsAsync()
	{
		var dbName = "test-db-create-options-async-x";
		var options = new DatabaseCreationOptions();
		options.Name = dbName;
		var admin = await fixture.Client.GetAstraDatabasesAdmin().CreateDatabaseAsync(options, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseByName
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void DropDatabaseByName()
	{
		var dbName = "test-db-drop-by-name";
		var dropped = fixture.Client.GetAstraDatabasesAdmin().DropDatabase(dbName);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseByNameAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DropDatabaseByNameAsync()
	{
		var dbName = "test-db-drop-by-name-async";
		var dropped = await fixture.Client.GetAstraDatabasesAdmin().DropDatabaseAsync(dbName);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseById
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void DropDatabaseById()
	{
		var dbGuid = Guid.Parse("ee1a268c-112f-47fd-971e-57ecef64a23b"); // from a db created ad-hoc on astra's site
		var dropped = fixture.Client.GetAstraDatabasesAdmin().DropDatabase(dbGuid);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DropDatabaseByIdAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DropDatabaseByIdAsync()
	{
		var dbGuid = Guid.Parse("65b4cdb5-2f21-4550-99ce-8c2570d18c1a"); // from a db created ad-hoc on astra's site
		var dropped = await fixture.Client.GetAstraDatabasesAdmin().DropDatabaseAsync(dbGuid);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DatabaseAdminAstra_CreateKeyspace_ExpectedError
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_CreateKeyspace_ExpectedError()
	{
		var databaseId = fixture.DatabaseId;
		var database = await fixture.Client.GetDatabaseAsync(databaseId);
		var adminOptions = new CommandOptions();
		var daa = new DatabaseAdminAstra(database, fixture.Client, adminOptions);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(
			() => daa.CreateKeyspaceAsync("default_keyspace")
		);
		Assert.Contains("Keyspace default_keyspace already exists", ex.Message);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DatabaseAdminAstra_CreateKeyspaceAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_CreateKeyspaceAsync()
	{
		var keyspaceName = "drop_this_keyspace_x";
		var adminOptions = new CommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.DatabaseId, fixture.Client, adminOptions);

		await daa.CreateKeyspaceAsync(keyspaceName, adminOptions);
		Console.WriteLine($"DatabaseAdminAstra_CreateKeyspaceAsync > adminOptions.Keyspace: {adminOptions.Keyspace}");
		Assert.Null(adminOptions.Keyspace);
		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DatabaseAdminAstra_CreateKeyspaceAsync_Update
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_CreateKeyspaceAsync_Update()
	{
		var keyspaceName = "drop_this_keyspace_x";
		var adminOptions = new CommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.DatabaseId, fixture.Client, adminOptions);

		await daa.CreateKeyspaceAsync(keyspaceName, true, adminOptions);

		Console.WriteLine($"DatabaseAdminAstra_CreateKeyspaceAsync_Update > adminOptions.Keyspace: {adminOptions.Keyspace}");
		Assert.Equal(keyspaceName, adminOptions.Keyspace);
		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests.DatabaseAdminAstra_DropKeyspaceAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_DropKeyspaceAsync()
	{
		var keyspaceName = "drop_this_keyspace_x";
		var adminOptions = new CommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.DatabaseId, fixture.Client, adminOptions);

		await daa.DropKeyspaceAsync(keyspaceName, adminOptions);
		// todo: better test result here; for now we assume if no error, this was successful
	}
}
