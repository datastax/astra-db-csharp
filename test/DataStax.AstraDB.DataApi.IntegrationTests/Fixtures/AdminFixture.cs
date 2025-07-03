using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using System.Text.RegularExpressions;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

public class AdminFixture : BaseFixture
{
	public string DatabaseName { get; set; }
	public Guid DatabaseId { get; set; }

	public AdminFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "admin")
	{
		DatabaseName = assemblyFixture.DatabaseName;
		DatabaseId = GetDatabaseIdFromUrl(assemblyFixture.DatabaseUrl).Value;
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

	public static Guid? GetDatabaseIdFromUrl(string url)
	{
		if (string.IsNullOrWhiteSpace(url))
			return null;

		// Match the first UUID in the URL
		var match = Regex.Match(url, @"([0-9a-fA-F-]{36})");
		return match.Success ? Guid.Parse(match.Value) : null;
	}

}
