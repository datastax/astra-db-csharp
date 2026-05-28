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
    public async Task GetDatabasesListNoOptions()
    {
        var list = await fixture.Client.GetAstraDatabasesAdmin().ListDatabasesAsync();
        Assert.NotNull(list);

        list = fixture.Client.GetAstraDatabasesAdmin().ListDatabases();
        Assert.NotNull(list);

        Console.WriteLine($"GetDatabasesList: {list.Count} items");
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task GetDatabasesListPartialOptions()
    {
        var list = await fixture.Client.GetAstraDatabasesAdmin().ListDatabasesAsync(new ListDatabaseOptions {
            StatesToInclude = QueryDatabaseStates.pending, PageSizeLimit = 41 });
        Assert.NotNull(list);

        list = fixture.Client.GetAstraDatabasesAdmin().ListDatabases();
        Assert.NotNull(list);

        Console.WriteLine($"GetDatabasesList: {list.Count} items");
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task GetDatabasesListWithOptions()
    {
        var list = await fixture.Client.GetAstraDatabasesAdmin().ListDatabasesAsync(new ListDatabaseOptions {
            StatesToInclude = QueryDatabaseStates.pending, Provider = QueryCloudProvider.AZURE, PageSizeLimit = 41, StartingAfter = "a" });
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
        var dbId = fixture.DatabaseId.ToString();

        var found = await fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExistAsync(dbId);
        Assert.True(found);

        found = fixture.Client.GetAstraDatabasesAdmin().DoesDatabaseExist(dbId);
        Assert.True(found);
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task CheckDatabaseStatus()
    {
        var dbGuid = Database.GetDatabaseIdFromUrl(fixture.DatabaseUrl).Value.ToString();
        Assert.NotNull(dbGuid);

        var status = await fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatusAsync(dbGuid);
        Assert.Equal(AstraDatabaseStatus.ACTIVE, status);

        status = await fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatusAsync(dbGuid, new GetDatabaseStatusOptions());
        Assert.Equal(AstraDatabaseStatus.ACTIVE, status);
    }

    [SkipWhenNotAstra]
    [Fact]
    public void DatabaseAdminAstra_GetDatabasesAdminAstra()
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
    public void DatabaseAdmin_GetAPIEndpoint()
    {
        var daa = fixture.CreateAdmin(fixture.Database) as DatabaseAdminAstra;

        Assert.Equal(fixture.DatabaseId, AdminFixture.GetDatabaseIdFromUrl(daa.GetAPIEndpoint()));
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

    [SkipWhenNotAstra]
    [Fact]
    public async Task DatabaseAdmin_GetAdmin_TypedPatterns_Astra()
    {
        var database = fixture.Client.GetDatabase(fixture.DatabaseUrl, fixture.Token);
        var options = new GetAdminOptions();

        var genAdmin = database.GetAdmin();
        var astraAdmin = database.GetAdmin<DatabaseAdminAstra>();
        var genAdminO = database.GetAdmin(options);
        var astraAdminO = database.GetAdmin<DatabaseAdminAstra>(options);

        Assert.IsType<DatabaseAdminAstra>(astraAdmin);
        Assert.Throws<ArgumentException>(() =>
            database.GetAdmin<DatabaseAdminDataAPI>()
        );
        Assert.IsType<DatabaseAdminAstra>(astraAdminO);
        Assert.Throws<ArgumentException>(() =>
            database.GetAdmin<DatabaseAdminDataAPI>(options)
        );
    }

    [SkipWhenAstra]
    [Fact]
    public async Task DatabaseAdmin_GetAdmin_TypedPatterns_NonAstra()
    {
        var database = fixture.Client.GetDatabase(fixture.DatabaseUrl, fixture.Token);
        var options = new GetAdminOptions();

        var genAdmin = database.GetAdmin();
        var hcdAdmin = database.GetAdmin<DatabaseAdminDataAPI>();
        var genAdminO = database.GetAdmin(options);
        var hcdAdminO = database.GetAdmin<DatabaseAdminDataAPI>(options);

        Assert.IsType<DatabaseAdminDataAPI>(hcdAdmin);
        Assert.Throws<ArgumentException>(() =>
            database.GetAdmin<DatabaseAdminAstra>()
        );
        Assert.IsType<DatabaseAdminDataAPI>(hcdAdminO);
        Assert.Throws<ArgumentException>(() =>
            database.GetAdmin<DatabaseAdminAstra>(options)
        );
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

        var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = await daa.FindEmbeddingProvidersAsync();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.EmbeddingProviders);

        // options with token
        var result_tk = await daa.FindEmbeddingProvidersAsync( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.EmbeddingProviders);

        // options with token and filter
        var result_tf = await daa.FindEmbeddingProvidersAsync( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.EmbeddingProviders);

        // options with filter
        var result_fi = await daa.FindEmbeddingProvidersAsync( new FindEmbeddingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.EmbeddingProviders);

    }

    [SkipWhenNotAstra]
    [Fact]
    public void DatabaseAdminAstra_FindEmbeddingProvidersSync()
    {

        var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = daa.FindEmbeddingProviders();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.EmbeddingProviders);

        // options with token
        var result_tk = daa.FindEmbeddingProviders( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.EmbeddingProviders);

        // options with token and filter
        var result_tf = daa.FindEmbeddingProviders( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.EmbeddingProviders);

        // options with filter
        var result_fi = daa.FindEmbeddingProviders( new FindEmbeddingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.EmbeddingProviders);

    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task DatabaseAdminAstra_FindRerankingProvidersAsync()
    {

        var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = await daa.FindRerankingProvidersAsync();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.RerankingProviders);

        // options with token
        var result_tk = await daa.FindRerankingProvidersAsync( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.RerankingProviders);

        // options with token and filter
        var result_tf = await daa.FindRerankingProvidersAsync( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.RerankingProviders);

        // options with filter
        var result_fi = await daa.FindRerankingProvidersAsync( new FindRerankingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.RerankingProviders);

    }

    [SkipWhenNotAstra]
    [Fact]
    public void DatabaseAdminAstra_FindRerankingProvidersSync()
    {

        var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = daa.FindRerankingProviders();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.RerankingProviders);

        // options with token
        var result_tk = daa.FindRerankingProviders( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.RerankingProviders);

        // options with token and filter
        var result_tf = daa.FindRerankingProviders( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.RerankingProviders);

        // options with filter
        var result_fi = daa.FindRerankingProviders( new FindRerankingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.RerankingProviders);

    }

    [SkipWhenAstra]
    [Fact]
    public async Task DatabaseAdminDataAPI_FindEmbeddingProvidersAsync()
    {

        var daa = new DatabaseAdminDataAPI(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = await daa.FindEmbeddingProvidersAsync();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.EmbeddingProviders);

        // options with token
        var result_tk = await daa.FindEmbeddingProvidersAsync( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.EmbeddingProviders);

        // options with token and filter
        var result_tf = await daa.FindEmbeddingProvidersAsync( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.EmbeddingProviders);

        // options with filter
        var result_fi = await daa.FindEmbeddingProvidersAsync( new FindEmbeddingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.EmbeddingProviders);

    }

    [SkipWhenAstra]
    [Fact]
    public void DatabaseAdminDataAPI_FindEmbeddingProvidersSync()
    {

        var daa = new DatabaseAdminDataAPI(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = daa.FindEmbeddingProviders();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.EmbeddingProviders);

        // options with token
        var result_tk = daa.FindEmbeddingProviders( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.EmbeddingProviders);

        // options with token and filter
        var result_tf = daa.FindEmbeddingProviders( new FindEmbeddingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.EmbeddingProviders);

        // options with filter
        var result_fi = daa.FindEmbeddingProviders( new FindEmbeddingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.EmbeddingProviders);

    }

    [SkipWhenAstra]
    [Fact]
    public async Task DatabaseAdminDataAPI_FindRerankingProvidersAsync()
    {

        var daa = new DatabaseAdminDataAPI(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = await daa.FindRerankingProvidersAsync();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.RerankingProviders);

        // options with token
        var result_tk = await daa.FindRerankingProvidersAsync( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.RerankingProviders);

        // options with token and filter
        var result_tf = await daa.FindRerankingProvidersAsync( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.RerankingProviders);

        // options with filter
        var result_fi = await daa.FindRerankingProvidersAsync( new FindRerankingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.RerankingProviders);

    }

    [SkipWhenAstra]
    [Fact]
    public void DatabaseAdminDataAPI_FindRerankingProvidersSync()
    {

        var daa = new DatabaseAdminDataAPI(fixture.Database, fixture.Client,
            new CommandOptions { Token = fixture.Client.ClientOptions.Token });

        // no options
        var result_no = daa.FindRerankingProviders();
        Assert.NotNull(result_no);
        Assert.NotNull(result_no.RerankingProviders);

        // options with token
        var result_tk = daa.FindRerankingProviders( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token });
        Assert.NotNull(result_tk);
        Assert.NotNull(result_tk.RerankingProviders);

        // options with token and filter
        var result_tf = daa.FindRerankingProviders( new FindRerankingProvidersOptions {
            Token = fixture.Client.ClientOptions.Token,
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_tf);
        Assert.NotNull(result_tf.RerankingProviders);

        // options with filter
        var result_fi = daa.FindRerankingProviders( new FindRerankingProvidersOptions {
            FilterModelStatus = ModelLifecycleStatus.EndOfLife });
        Assert.NotNull(result_fi);
        Assert.NotNull(result_fi.RerankingProviders);

    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task DatabaseAdminAstra_GetRegions()
    {
        var admin = fixture.Client.GetAstraDatabasesAdmin();

        var regionsDefault = await admin.FindAvailableRegionsAsync();
        Assert.NotNull(regionsDefault);
        Assert.NotEmpty(regionsDefault);

        var regionsOnly = await admin.FindAvailableRegionsAsync(new FindAvailableRegionsOptions {
            OnlyOrgEnabledRegions = true
        });
        Assert.NotNull(regionsOnly);
        Assert.NotEmpty(regionsOnly);

        var regionsAll = await admin.FindAvailableRegionsAsync(new FindAvailableRegionsOptions {
            OnlyOrgEnabledRegions = false
        });
        Assert.NotNull(regionsAll);
        Assert.NotEmpty(regionsAll);

        Assert.True(regionsAll.Count >= regionsOnly.Count);
    }

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
                }
            )
        );
        Assert.Contains("Value cannot be null or empty", ex1.Message);

        var ex2 = Assert.Throws<ArgumentNullException>(() =>
            fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(
                new (){
                    CloudProvider = CloudProviderType.AWS,
                    Region = "the-region-",
                    Keyspace = "fedault_seykpace"
                }
            )
        );
        Assert.Contains("Value cannot be null or empty", ex2.Message);

        var ex3 = Assert.Throws<ArgumentNullException>(() =>
            fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(
                new (){
                    Name = "theoretical_db_3",
                    Region = "the-region-",
                    Keyspace = "fedault_seykpace"
                }
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

        Also, make sure the DB creation/deletion parameters follow the env being tested:
            DEV: GCP europe-west4
            TEST: AWS us-east-1
        For dropping, you will need to hardcode the proper database Guid in the test.
    */

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabaseNonblockingSync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task CreateDatabaseNonblockingSync()
    {
        var dbName = "test-db-create-x";
        var creationOptions = new BlockingCommandOptions() {
            waitForCompletion = false,
        };
        var admin = fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(
            new (){
                Name = dbName,
                CloudProvider = CloudProviderType.GCP,
                Region = "europe-west4",
                Keyspace = "fedault_seykpace"
            },
            creationOptions
        );

        var dbStatus = fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatus(admin.Id);
        Assert.True(dbStatus == AstraDatabaseStatus.ASSOCIATING
            || dbStatus == AstraDatabaseStatus.INITIALIZING
            || dbStatus == AstraDatabaseStatus.PENDING);
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabaseNonblockingAsync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task CreateDatabaseNonblockingAsync()
    {
        var dbName = "test-db-create-async-x";
        var options = new DatabaseCreationOptions{
            Name = dbName,
            CloudProvider = CloudProviderType.GCP,
            Region = "europe-west4"
        };
        var creationOptions = new BlockingCommandOptions() {
            waitForCompletion = false,
        };
        var admin = await fixture.Client.GetAstraDatabasesAdmin().CreateDatabaseAsync(options, creationOptions);

        var dbStatus = await fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatusAsync(admin.Id);
        Assert.True(dbStatus == AstraDatabaseStatus.ASSOCIATING
            || dbStatus == AstraDatabaseStatus.INITIALIZING
            || dbStatus == AstraDatabaseStatus.PENDING);
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabaseBlockingSync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public void CreateDatabaseBlockingSync()
    {
        var dbName = "test-db-create-blocking-x";
        var options = new DatabaseCreationOptions{
            Name = dbName,
            CloudProvider = CloudProviderType.GCP,
            Region = "europe-west4",
            Keyspace = "fedault_seykpace"
        };
        var creationOptions = new BlockingCommandOptions() {
            waitForCompletion = true,
        };
        var admin = fixture.Client.GetAstraDatabasesAdmin().CreateDatabase(options, creationOptions);

        var dbStatus = fixture.Client.GetAstraDatabasesAdmin().GetDatabaseStatus(admin.Id);
        Assert.True(dbStatus == AstraDatabaseStatus.ACTIVE);
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.CreateDatabaseBlockingAsync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task CreateDatabaseBlockingAsync()
    {
        var dbName = "test-db-create-blocking-async-x";
        // this one tests the "blocking by default" pattern:
        var admin = await fixture.Client.GetAstraDatabasesAdmin().CreateDatabaseAsync(
            new (){
                Name = dbName,
                CloudProvider = CloudProviderType.GCP,
                Region = "europe-west4"
            }
        );

        // verify by creating a keyspace (devops), listing it (devops) and listing the DB tables (data api)
        await admin.CreateKeyspaceAsync("throwaway_ks");
        Assert.True(admin.DoesKeyspaceExist("throwaway_ks"));
        var database = admin.GetDatabase();
        var tableNames = await database.ListTableNamesAsync();
        Assert.NotNull(tableNames);
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabaseNonblockingSync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public void DropDatabaseNonblockingSync()
    {
        var waitingOptions = new BlockingCommandOptions
        {
            waitForCompletion = false,
        };
        var dbGuid = "06279ec0-c17c-498d-8bc4-b46cc04a2a71"; // tester must supply the Guid for a pre-created DB
        fixture.Client.GetAstraDatabasesAdmin().DropDatabase(dbGuid, waitingOptions);
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabaseNonblockingAsync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task DropDatabaseNonblockingAsync()
    {
        var waitingOptions = new BlockingCommandOptions
        {
            waitForCompletion = false,
        };
        var dbGuid = "6a118896-bd69-4f24-90db-6229cd211c99"; // tester must supply the Guid for a pre-created DB
        await fixture.Client.GetAstraDatabasesAdmin().DropDatabaseAsync(dbGuid, waitingOptions);
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabaseBlockingSync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public void DropDatabaseBlockingSync()
    {
        // this one tests the 'blocking by default' pattern:
        var dbGuid = "4542468f-fcc8-4830-a8e6-b2acb65694be"; // tester must supply the Guid for a pre-created DB
        fixture.Client.GetAstraDatabasesAdmin().DropDatabase(dbGuid);
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DropDatabaseBlockingAsync
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task DropDatabaseBlockingAsync()
    {
        // this one explicitly requires the blocking call:
        var waitingOptions = new BlockingCommandOptions
        {
            waitForCompletion = true,
        };
        var dbGuid = "37565911-f051-471a-9dc1-90a9eab76295"; // tester must supply the Guid for a pre-created DB
        await fixture.Client.GetAstraDatabasesAdmin().DropDatabaseAsync(dbGuid, waitingOptions);
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

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminAstra_CreateKeyspaceAsync_Update
    [SkipWhenNotAstra]
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task DatabaseAdminAstra_CreateKeyspaceAsync_Update()
    {
        var keyspaceName = "drop_this_keyspace_x";
        var adminOptions = new BlockingCommandOptions
        {
            Token = fixture.Client.ClientOptions.Token,
        };
        // LCN fixDB on 'default_keyspace'
        await fixture.Database.ListCollectionNamesAsync();

        var theDatabase = fixture.Client.GetDatabase(fixture.DatabaseUrl,
            new DatabaseCommandOptions() { Keyspace = "some_throwaway_keyspace_name" });
        Assert.Equal("some_throwaway_keyspace_name", theDatabase.Keyspace);
        theDatabase.UseKeyspace("another_silly_puppet_keyspace");
        Assert.Equal("another_silly_puppet_keyspace", theDatabase.Keyspace);
        var daa = new DatabaseAdminAstra(theDatabase, fixture.Client, adminOptions);

        await daa.CreateKeyspaceAsync(keyspaceName, new () {updateDBKeyspace = true});
        Assert.Equal(keyspaceName, theDatabase.Keyspace);
        // LCN myDB on keyspaceName
        await theDatabase.ListCollectionNamesAsync();

        theDatabase.UseKeyspace(Database.DefaultKeyspace);
        Assert.Equal(Database.DefaultKeyspace, theDatabase.Keyspace);
        // LCN myDB on 'default_keyspace'
        await theDatabase.ListCollectionNamesAsync();

        theDatabase.UseKeyspace(keyspaceName);
        Assert.Equal(keyspaceName, theDatabase.Keyspace);
        // LCN myDB on keyspaceName
        await theDatabase.ListCollectionNamesAsync();

        fixture.Database.UseKeyspace(keyspaceName);
        Assert.Equal(keyspaceName, fixture.Database.Keyspace);
        // LCN fixDB on keyspaceName
        await fixture.Database.ListCollectionNamesAsync();

        fixture.Database.UseKeyspace(Database.DefaultKeyspace);
        Assert.Equal(Database.DefaultKeyspace, fixture.Database.Keyspace);
        // LCN fixDB on 'default_keyspace'
        await fixture.Database.ListCollectionNamesAsync();

        Assert.Contains(keyspaceName, daa.ListKeyspaces());
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminDataAPI_CreateKeyspaceAsync_Update
    [SkipWhenAstra]
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task DatabaseAdminDataAPI_CreateKeyspaceAsync_Update()
    {
        var keyspaceName = "drop_this_keyspace_x";
        var adminOptions = new BlockingCommandOptions
        {
            Token = fixture.Client.ClientOptions.Token,
        };
        var ckOptions = new CreateKeyspaceOptions
        {
            updateDBKeyspace = true,
        };
        // LCN fixDB on 'default_keyspace'
        await fixture.Database.ListCollectionNamesAsync();

        var theDatabase = fixture.Client.GetDatabase(fixture.DatabaseUrl,
            new DatabaseCommandOptions() { Keyspace = "some_throwaway_keyspace_name" });
        Assert.Equal("some_throwaway_keyspace_name", theDatabase.Keyspace);
        theDatabase.UseKeyspace("another_silly_puppet_keyspace");
        Assert.Equal("another_silly_puppet_keyspace", theDatabase.Keyspace);
        var daa = new DatabaseAdminDataAPI(theDatabase, fixture.Client, adminOptions);

        await daa.CreateKeyspaceAsync(keyspaceName, ckOptions);
        Assert.Equal(keyspaceName, theDatabase.Keyspace);
        // LCN myDB on keyspaceName
        await theDatabase.ListCollectionNamesAsync();

        theDatabase.UseKeyspace(Database.DefaultKeyspace);
        Assert.Equal(Database.DefaultKeyspace, theDatabase.Keyspace);
        // LCN myDB on 'default_keyspace'
        await theDatabase.ListCollectionNamesAsync();

        theDatabase.UseKeyspace(keyspaceName);
        Assert.Equal(keyspaceName, theDatabase.Keyspace);
        // LCN myDB on keyspaceName
        await theDatabase.ListCollectionNamesAsync();

        fixture.Database.UseKeyspace(keyspaceName);
        Assert.Equal(keyspaceName, fixture.Database.Keyspace);
        // LCN fixDB on keyspaceName
        await fixture.Database.ListCollectionNamesAsync();

        fixture.Database.UseKeyspace(Database.DefaultKeyspace);
        Assert.Equal(Database.DefaultKeyspace, fixture.Database.Keyspace);
        // LCN fixDB on 'default_keyspace'
        await fixture.Database.ListCollectionNamesAsync();

        Assert.Contains(keyspaceName, daa.ListKeyspaces());

        // HCD-specific one-liner idiom
        var swiftDatabase = fixture.Client.GetDatabase(fixture.DatabaseUrl,
            new DatabaseCommandOptions() { Keyspace = "some_throwaway_keyspace_name" });
        Assert.NotEqual(keyspaceName, swiftDatabase.Keyspace);
        await swiftDatabase.GetAdmin().CreateKeyspaceAsync(keyspaceName, ckOptions);
        Assert.Equal(keyspaceName, swiftDatabase.Keyspace);

    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminDataAPI_CreateKeyspaceAsync_WithOptions
    [SkipWhenAstra]
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task DatabaseAdminDataAPI_CreateKeyspaceAsync_WithOptions()
    {
        var keyspaceName = "throwaway_keyspace_with_options";
        var adminOptions = new BlockingCommandOptions
        {
            Token = fixture.Client.ClientOptions.Token,
        };
        var ckOptions = new CreateKeyspaceOptions
        {
            Token = fixture.Client.ClientOptions.Token,
        };
        var daa = new DatabaseAdminDataAPI(fixture.Database, fixture.Client, adminOptions);

        var replicationOptions = new Dictionary<string, object> { ["class"] = "SimpleStrategy", ["replication_factor"] = 1 };
        await daa.CreateKeyspaceAsync(keyspaceName, replicationOptions, ckOptions);

        Assert.Contains(keyspaceName, daa.ListKeyspaces());
    }

    // dotnet test --filter FullyQualifiedName=DatSaStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminAstra_DropKeyspaceAsync
    [SkipWhenNotAstra]
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task DatabaseAdminAstra_DropKeyspaceAsync()
    {
        var keyspaceName = "drop_this_keyspace_x";
        var adminOptions = new DropKeyspaceOptions
        {
            Token = fixture.Client.ClientOptions.Token,
        };
        var daa = new DatabaseAdminAstra(fixture.Database, fixture.Client, adminOptions);

        Assert.True(daa.DoesKeyspaceExist(keyspaceName));
        await daa.DropKeyspaceAsync(keyspaceName, adminOptions);
        Assert.False(daa.DoesKeyspaceExist(keyspaceName));
    }

    // dotnet test --filter FullyQualifiedName=DataStax.AstraDB.DataApi.IntegrationTests.AdminTests.DatabaseAdminDataAPI_DropKeyspaceAsync
    [SkipWhenAstra]
    [Fact(Skip = AdminCollection.SkipMessage)]
    public async Task DatabaseAdminDataAPI_DropKeyspaceAsync()
    {
        var keyspaceName = "drop_this_keyspace_x";
        var adminOptions = new DropKeyspaceOptions
        {
            Token = fixture.Client.ClientOptions.Token,
        };
        var daa = new DatabaseAdminDataAPI(fixture.Database, fixture.Client, adminOptions);

        Assert.True(daa.DoesKeyspaceExist(keyspaceName));
        await daa.DropKeyspaceAsync(keyspaceName, adminOptions);
        Assert.False(daa.DoesKeyspaceExist(keyspaceName));
    }
}
