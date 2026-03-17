using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[CollectionDefinition("Admin Collection")]
public class AdminCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<AdminFixture>
{
	public const string SkipMessage = "Please read 'How to run these skipped tests'";
}

//  dotnet test --filter "FullyQualifiedName~DataStax.AstraDB.DataApi.IntegrationTests.Tests.AdminTests"
[Collection("Admin Collection")]
public class AdminTests
{
	AdminFixture fixture;

	public AdminTests(AssemblyFixture assemblyFixture, AdminFixture fixture)
	{
		this.fixture = fixture;
	}

	[SkipWhenNotAstra]
	[Fact]
	public async Task GetDatabasesList()
	{
		var list = await fixture.Client.GetAstraDatabasesAdmin().ListDatabasesAsync();
		Assert.NotNull(list);

		list = fixture.Client.GetAstraDatabasesAdmin().ListDatabases();
		Assert.NotNull(list);

		Console.WriteLine($"GetDatabasesList: {list.Count} items");
	}

	[SkipWhenNotAstra]
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

	[SkipWhenNotAstra]
	[Fact]
	public async Task CheckDatabaseExistsByName()
	{
		var dbName = fixture.DatabaseName;
		if (string.IsNullOrEmpty(dbName))
		{
			Console.WriteLine("Skipping CheckDatabaseExistsByName due to missing DATABASE_NAME param");
		}

		var found = await fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExistAsync(dbName);
		Assert.True(found);

		found = fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(dbName);
		Assert.True(found);
	}

	[SkipWhenNotAstra]
	[Theory]
	[InlineData(null)]
	[InlineData("")]
	public void CheckDatabaseExistsByName_ExpectedError(string invalidName)
	{
		var ex = Assert.Throws<ArgumentNullException>(() => fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(invalidName));
		Assert.Contains("Value cannot be null or empty", ex.Message);
	}

	[SkipWhenNotAstra]
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

	[SkipWhenNotAstra]
	[Fact]
	public async Task CheckDatabaseExistsByName_ExpectedFalse()
	{
		var dbName = "this-is-not-the-greatest-db-in-the-world-this-is-a-tribute";

		var databasesAdmin = fixture.Client.GetAstraDatabasesAdmin();
		var doesExist = await databasesAdmin.DoesDatabaseExistAsync(dbName);
		Assert.False(doesExist);

		doesExist = databasesAdmin.DoesDatabaseExist(dbName);
		Assert.False(doesExist);
	}

	[SkipWhenNotAstra]
	[Fact]
	public async Task CheckDatabaseExistsById()
	{
		var dbId = fixture.DatabaseId;

		var found = await fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExistAsync(dbId);
		Assert.True(found);

		found = fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(dbId);
		Assert.True(found);
	}

	[SkipWhenNotAstra]
	[Fact]
	public async Task CheckDatabaseStatus()
	{
		var dbGuid = Database.GetDatabaseIdFromUrl(fixture.DatabaseUrl);
		Assert.NotNull(dbGuid);

		var status = await fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatusAsync(dbGuid.Value);
		Assert.Equal(AstraDatabaseStatus.ACTIVE, status);

		status = await fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatusAsync(dbGuid.Value);
		Assert.Equal(AstraDatabaseStatus.ACTIVE, status);
	}

	[SkipWhenNotAstra]
	[Fact]
	public void DatabaseAdminAstra_GetDatabaseAdminAstra()
	{
		var daa = fixture.Client.GetAstraDatabasesAdmin();

		Assert.IsType<AstraDatabasesAdmin>(daa);
	}

	[SkipWhenNotAstra]
	[Fact]
	public void DatabaseAdmin_GetDatabase()
	{
		var daa = fixture.CreateAdmin(fixture.Database);

		Assert.IsType<Database>(daa.GetDatabase());
	}

	[SkipWhenNotAstra]
	[Fact]
	public void DatabaseAdmin_GetApiEndpoint()
	{
		var daa = fixture.CreateAdmin(fixture.Database) as DatabaseAdminAstra;

		Assert.Equal(fixture.DatabaseId, AdminFixture.GetDatabaseIdFromUrl(daa.GetApiEndpoint()));
	}

	[Fact]
	public async Task DatabaseAdmin_GetKeyspacesList()
	{
		var daa = fixture.CreateAdmin(fixture.Database);

		var names = await daa.ListKeyspacesAsync();
		Assert.NotNull(names);

		names = daa.ListKeyspaces();
		Assert.NotNull(names);

		var list = names.ToList();
		Console.WriteLine($"ListKeyspaces: {list.Count} items");
		list.ForEach(Console.WriteLine);
	}

	[Fact]
	public async Task DatabaseAdmin_GetKeyspaces_TokenSuppliedToDb()
	{
		var database = fixture.Client.GetDatabase(fixture.DatabaseUrl, fixture.Token);
		var databaseAdmin = database.GetAdmin();

		var names = await databaseAdmin.ListKeyspacesAsync();
		Assert.NotNull(names);
	}

	[Fact]
	public async Task DatabaseAdmin_DoesKeyspaceExist()
	{
		var daa = fixture.CreateAdmin(fixture.Database);

		var keyspaceExists = await daa.DoesKeyspaceExistAsync("default_keyspace");
		Assert.True(keyspaceExists);

		keyspaceExists = await daa.DoesKeyspaceExistAsync("default_keyspace");
		Assert.True(keyspaceExists);
	}

	[Fact]
	public async Task DatabaseAdmin_DoesKeyspaceExist_Another()
	{
		var daa = fixture.CreateAdmin(fixture.Database);
		var keyspaceName = "another_keyspace";

		try
		{
			var keyspaceExists = await daa.DoesKeyspaceExistAsync(keyspaceName);
			if (!keyspaceExists)
			{
				await daa.CreateKeyspaceAsync(keyspaceName);
				await Task.Delay(30 * 1000, TestContext.Current.CancellationToken); //wait for keyspace to be created
				keyspaceExists = await daa.DoesKeyspaceExistAsync(keyspaceName);
			}
			Assert.True(keyspaceExists);
		}
		finally
		{
			await daa.DropKeyspaceAsync(keyspaceName);
		}
	}

	[SkipWhenNotAstra]
	[Fact]
	public async Task DatabaseAdminAstra_FindEmbeddingProvidersAsync()
	{
		var adminOptions = new FindEmbeddingProvidersCommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client, adminOptions);

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

	[SkipWhenNotAstra]
	[Fact]
	public async Task DatabaseAdminAstra_FindRerankingProvidersAsync()
	{
		var adminOptions = new FindRerankingProvidersCommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
			
		};
		var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client, adminOptions);

		var result = await daa.FindRerankingProvidersAsync(adminOptions, runSynchronously: false);
		Assert.NotNull(result);
		var providers = result.RerankingProviders;

		Assert.NotNull(providers);
		Assert.NotEmpty(providers);
	}

	[SkipWhenNotAstra]
	[Fact]
	public async Task DatabaseAdminAstra_GetRegions()
	{
		var admin = fixture.Client.GetAstraDatabasesAdmin();

		var regions = await admin.FindAvailableRegionsAsync();
		Assert.NotNull(regions);
		Assert.NotEmpty(regions);
	}

	// [SkipWhenNotAstra] TODO uncomment when FARR is added back
	// [Fact]
	// public async Task DatabaseAdminAstra_GetRerankingProvidersAsync()
	// {
	// 	var adminOptions = new CommandOptions
	// 	{
	// 		Token = fixture.Client.ClientOptions.Token,
	// 	};
	// 	var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client, adminOptions);
	//
	// 	var result = await daa.FindRerankingProvidersAsync();
	// 	Assert.NotNull(result);
	// 	Assert.NotEmpty(result.RerankingProviders);
	//
	// }

	[SkipWhenNotAstra]
	[Fact()]
	public void CreateDatabaseMissingParameters()
	{
		var ex1 = Assert.Throws<ArgumentNullException>(() =>
			fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(
				new (){
					Name = "theoretical_db_1",
					CloudProvider = CloudProviderType.AWS,
					Keyspace = "fedault_seykpace"
				},
				false
			)
		);
		Assert.Contains("Value cannot be null or empty", ex1.Message);

		var ex2 = Assert.Throws<ArgumentNullException>(() =>
			fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(
				new (){
					CloudProvider = CloudProviderType.AWS,
					Region = "the-region-",
					Keyspace = "fedault_seykpace"
				},
				false
			)
		);
		Assert.Contains("Value cannot be null or empty", ex2.Message);

		var ex3 = Assert.Throws<ArgumentNullException>(() =>
			fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(
				new (){
					Name = "theoretical_db_3",
					Region = "the-region-",
					Keyspace = "fedault_seykpace"
				},
				false
			)
		);
		Assert.Contains("Value cannot be null", ex3.Message);
	}

	/*
        From here on are ad hoc tests for creating and dropping databases. 
        You will likely need to adjust details to match your execution details.

        How to run these skipped tests:
        1. Comment the attribute with the skip.
        2. Add a [Fact] attribute.
        3. Run the associated command from the terminal.
    */

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabase
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void CreateDatabase()
	{
		var dbName = "test-db-create-x";
		var admin = fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(
			new (){
				Name = dbName,
				CloudProvider = CloudProviderType.AWS,
				Region = "us-east-2",
				Keyspace = "fedault_seykpace"
			},
			false
		);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabaseAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseAsync()
	{
		var dbName = "test-db-create-async-x";
		var options = new DatabaseCreationOptions{
			Name = dbName,
			CloudProvider = CloudProviderType.AWS,
			Region = "us-east-2"
		};
		var admin = await fixture.Client.GetAstraDatabasesAdmin().CreateDatabaseAsync(options, false);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabaseBlocking
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void CreateDatabaseBlocking()
	{
		var dbName = "test-db-create-blocking-x";
		var options = new DatabaseCreationOptions{
			Name = dbName,
			CloudProvider = CloudProviderType.AWS,
			Region = "us-east-2",
			Keyspace = "fedault_seykpace"
		};
		var admin = fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(options, true);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabaseBlockingAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task CreateDatabaseBlockingAsync()
	{
		var dbName = "test-db-create-blocking-async-x";
		var admin = await fixture.Client.GetAstraDatabasesAdmin().CreateDatabaseAsync(
			new (){
				Name = dbName,
				CloudProvider = CloudProviderType.AWS,
				Region = "us-east-2"
			},
			true
		);

		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabase
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void DropDatabase()
	{
		var dbGuid = Guid.Parse("7683bb84-4604-49b4-b05f-69b695bba976"); // from a db created ad-hoc on astra's site
		var dropped = fixture.Client.GetAstraDatabasesAdmin().DropDatabase(dbGuid, false);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabaseAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DropDatabaseAsync()
	{
		var dbGuid = Guid.Parse("6a118896-bd69-4f24-90db-6229cd211c99"); // from a db created ad-hoc on astra's site
		var dropped = await fixture.Client.GetAstraDatabasesAdmin().DropDatabaseAsync(dbGuid, false);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabaseBlocking
	[Fact(Skip = AdminCollection.SkipMessage)]
	public void DropDatabaseBlocking()
	{
		var dbGuid = Guid.Parse("949c493b-0d08-41ca-b2e1-5a636c05f3ed"); // from a db created ad-hoc on astra's site
		var dropped = fixture.Client.GetAstraDatabasesAdmin().DropDatabase(dbGuid, true);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabaseBlockingAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DropDatabaseBlockingAsync()
	{
		var dbGuid = Guid.Parse("2b8bc268-511b-4b35-adfd-ef4f3063351b"); // from a db created ad-hoc on astra's site
		var dropped = await fixture.Client.GetAstraDatabasesAdmin().DropDatabaseAsync(dbGuid, true);

		Assert.True(dropped);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminAstra_CreateKeyspace_ExpectedError
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_CreateKeyspace_ExpectedError()
	{
		var adminOptions = new CommandOptions();
		var daa = fixture.CreateAdmin(fixture.Database);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(
			() => daa.CreateKeyspaceAsync("default_keyspace")
		);
		Assert.Contains("Keyspace default_keyspace already exists", ex.Message);
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminAstra_CreateKeyspaceAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_CreateKeyspaceAsync()
	{
		var keyspaceName = "drop_this_keyspace_x";
		var adminOptions = new CommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client, adminOptions);

		await daa.CreateKeyspaceAsync(keyspaceName, adminOptions);
		Console.WriteLine($"DatabaseAdminAstra_CreateKeyspaceAsync > adminOptions.Keyspace: {adminOptions.Keyspace}");
		Assert.Null(adminOptions.Keyspace);
		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminAstra_CreateKeyspaceAsync_Update
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_CreateKeyspaceAsync_Update()
	{
		var keyspaceName = "drop_this_keyspace_x";
		var adminOptions = new CommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client, adminOptions);

		await daa.CreateKeyspaceAsync(keyspaceName, true, adminOptions);

		Console.WriteLine($"DatabaseAdminAstra_CreateKeyspaceAsync_Update > adminOptions.Keyspace: {adminOptions.Keyspace}");
		Assert.Equal(keyspaceName, adminOptions.Keyspace);
		// todo: better test result here; for now we assume if no error, this was successful
	}

	// dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminAstra_DropKeyspaceAsync
	[Fact(Skip = AdminCollection.SkipMessage)]
	public async Task DatabaseAdminAstra_DropKeyspaceAsync()
	{
		var keyspaceName = "drop_this_keyspace_x";
		var adminOptions = new CommandOptions
		{
			Token = fixture.Client.ClientOptions.Token,
		};
		var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client, adminOptions);

		await daa.DropKeyspaceAsync(keyspaceName, adminOptions);
		// todo: better test result here; for now we assume if no error, this was successful
	}
}
