using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

public class AdminFixture : IDisposable
{
	public AdminFixture()
	{
		IConfiguration configuration = new ConfigurationBuilder()
			.SetBasePath(Directory.GetCurrentDirectory())
			.AddJsonFile("appsettings.json", optional: true)
			.AddEnvironmentVariables(prefix: "ASTRA_DB_")
			.Build();

		var token = configuration["ADMINTOKEN"] ?? configuration["AstraDB:AdminToken"];
		var dbUrl = configuration["URL"];
		DatabaseUrl = dbUrl;
		DatabaseName = configuration["DATABASE_NAME"];

		_databaseId = GetDatabaseIdFromUrl(dbUrl) ?? throw new Exception("Database ID could not be extracted from ASTRA_DB_URL.");

		using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddFileLogger("../../../_logs/admin_tests_latest_run.log"));
		ILogger logger = factory.CreateLogger("IntegrationTests");

		var clientOptions = new CommandOptions
		{
			RunMode = RunMode.Debug
		};
		Client = new DataApiClient(token, clientOptions, logger);
		Database = Client.GetDatabase(DatabaseUrl);
	}

	public void Dispose()
	{
		// ... clean up test data from the database ...
	}

	private readonly Guid _databaseId;
	public Guid DatabaseId => _databaseId;
	public string DatabaseName { get; private set; }
	public DataApiClient Client { get; private set; }
	public string DatabaseUrl { get; private set; }
	public Database Database { get; private set; }

	public Database GetDatabase()
	{
		return Client.GetDatabase(DatabaseUrl);
	}

	public static Guid? GetDatabaseIdFromUrl(string url)
	{
		if (string.IsNullOrWhiteSpace(url))
			return null;

		// Match the first UUID in the URL
		var match = Regex.Match(url, @"([0-9a-fA-F-]{36})");
		return match.Success ? Guid.Parse(match.Value) : null;
	}

	public DatabaseAdminAstra CreateAdmin(Database database = null)
	{
		database ??= Database;

		var adminOptions = new CommandOptions
		{
			Token = Client.ClientOptions.Token,
			Environment = DBEnvironment.Production // or default
		};

		return new DatabaseAdminAstra(database, Client, adminOptions);
	}

}
