using DataStax.AstraDB.DataApi;
using DataStax.AstraDB.DataApi.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Text.RegularExpressions;

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
		var dbUrl = configuration["URL"]; // ASTRA_DB_URL gets mapped to URL

		_databaseId = GetDatabaseIdFromUrl(dbUrl) ?? throw new Exception("Database ID could not be extracted from ASTRA_DB_URL.");

		using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
		ILogger logger = factory.CreateLogger("IntegrationTests");

		var clientOptions = new CommandOptions
		{
			RunMode = RunMode.Debug
		};
		Client = new DataApiClient(token, clientOptions, logger);
	}

	public void Dispose()
	{
		// ... clean up test data from the database ...
	}

	private readonly Guid _databaseId;
	public Guid DatabaseId => _databaseId;
	public DataApiClient Client { get; private set; }

	private static Guid? GetDatabaseIdFromUrl(string url)
	{
		if (string.IsNullOrWhiteSpace(url))
			return null;

		// Match the first UUID in the URL
		var match = Regex.Match(url, @"([0-9a-fA-F-]{36})");
		return match.Success? Guid.Parse(match.Value) : null;
	}
}
