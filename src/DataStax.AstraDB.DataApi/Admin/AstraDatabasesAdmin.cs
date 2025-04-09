/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Admin;

/// <summary>
/// Provides administrative operations for Astra databases.
/// </summary>
/// <example>
/// <code>
/// var admin = new AstraDatabasesAdmin(client, adminOptions);
/// </code>
/// </example>
public class AstraDatabasesAdmin
{
    private const int WAIT_IN_SECONDS = 600;

    private readonly CommandOptions _adminOptions;
    private readonly DataApiClient _client;

    private CommandOptions[] OptionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };

    internal AstraDatabasesAdmin(DataApiClient client, CommandOptions adminOptions)
    {
        Guard.NotNull(client, nameof(client));
        _client = client;
        _adminOptions = adminOptions;
    }

    /// <summary>
    /// Returns a list of database names.
    /// </summary>
    /// <returns>A list of database names.</returns>
    /// <example>
    /// <code>
    /// var names = admin.ListDatabaseNames();
    /// </code>
    /// </example>
    public List<string> ListDatabaseNames()
    {
        return ListDatabases().Select(db => db.Info.Name).ToList();
    }

    /// <summary>
    /// Asynchronously returns a list of database names.
    /// </summary>
    /// <returns>A task that resolves to a list of database names.</returns>
    /// <example>
    /// <code>
    /// var names = await admin.ListDatabaseNamesAsync();
    /// </code>
    /// </example>
    public async Task<List<string>> ListDatabaseNamesAsync()
    {
        var databases = await ListDatabasesAsync().ConfigureAwait(false);
        return databases.Select(db => db.Info.Name).ToList();
    }

    /// <summary>
    /// Returns a list of database names using specified command options.
    /// </summary>
    /// <param name="options">The command options to use.</param>
    /// <returns>A list of database names.</returns>
    /// <example>
    /// <code>
    /// var names = admin.ListDatabaseNames(options);
    /// </code>
    /// </example>
    public List<string> ListDatabaseNames(CommandOptions options)
    {
        return ListDatabases(options).Select(db => db.Info.Name).ToList();
    }

    /// <summary>
    /// Asynchronously returns a list of database names using specified command options.
    /// </summary>
    /// <param name="options">The command options to use.</param>
    /// <returns>A task that resolves to a list of database names.</returns>
    /// <example>
    /// <code>
    /// var names = await admin.ListDatabaseNamesAsync(options);
    /// </code>
    /// </example>
    public async Task<List<string>> ListDatabaseNamesAsync(CommandOptions options)
    {
        var databases = await ListDatabasesAsync(options).ConfigureAwait(false);
        return databases.Select(db => db.Info.Name).ToList();
    }

    /// <summary>
    /// Returns a list of database info objects.
    /// </summary>
    /// <returns>A list of DatabaseInfo objects.</returns>
    /// <example>
    /// <code>
    /// var databases = admin.ListDatabases();
    /// </code>
    /// </example>
    public List<DatabaseInfo> ListDatabases()
    {
        return ListDatabasesAsync(null, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously returns a list of database info objects.
    /// </summary>
    /// <returns>A task that resolves to a list of DatabaseInfo objects.</returns>
    /// <example>   
    /// <code>
    /// var databases = await admin.ListDatabasesAsync();
    /// </code>
    /// </example>
    public Task<List<DatabaseInfo>> ListDatabasesAsync()
    {
        return ListDatabasesAsync(null, false);
    }

    /// <summary>
    /// Returns a list of database info objects using specified command options.
    /// </summary>
    /// <param name="options">The command options to use.</param>
    /// <returns>A list of DatabaseInfo objects.</returns>
    /// <example>
    /// <code>
    /// var databases = admin.ListDatabases(options);
    /// </code>
    /// </example>
    public List<DatabaseInfo> ListDatabases(CommandOptions options)
    {
        return ListDatabasesAsync(options, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously returns a list of database info objects using specified command options.
    /// </summary>
    /// <param name="options">The command options to use.</param>
    /// <returns>A task that resolves to a list of DatabaseInfo objects.</returns>
    /// <example>
    /// <code>
    /// var databases = await admin.ListDatabasesAsync(options);
    /// </code>
    /// </example>
    public Task<List<DatabaseInfo>> ListDatabasesAsync(CommandOptions options)
    {
        return ListDatabasesAsync(options, false);
    }

    internal Task<List<DatabaseInfo>> ListDatabasesAsync(CommandOptions options, bool runSynchronously)
    {
        var command = CreateCommand()
            .AddUrlPath("databases")
            .AddCommandOptions(options);

        var response = command.RunAsyncRaw<List<DatabaseInfo>>(HttpMethod.Get, runSynchronously);
        return response;
    }

    /// <summary>
    /// Checks if a database with the specified name exists.
    /// </summary>
    /// <param name="databaseName">The database name to check.</param>
    /// <returns>True if the database exists; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool exists = admin.DoesDatabaseExist("myDatabase");
    /// </code>
    /// </example>
    public bool DoesDatabaseExist(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        List<string> list = ListDatabaseNames();
        return list.Contains(databaseName);
    }

    /// <summary>
    /// Asynchronously checks if a database with the specified name exists.
    /// </summary>
    /// <param name="databaseName">The database name to check.</param>
    /// <returns>A task that resolves to true if the database exists; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool exists = await admin.DoesDatabaseExistAsync("myDatabase");
    /// </code>
    /// </example>
    public async Task<bool> DoesDatabaseExistAsync(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        List<string> list = await ListDatabaseNamesAsync();
        return list.Contains(databaseName);
    }

    /// <summary>
    /// Checks if a database with the specified GUID exists.
    /// </summary>
    /// <param name="dbGuid">The database GUID to check.</param>
    /// <returns>True if the database exists; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool exists = admin.DoesDatabaseExist(new Guid("..."));
    /// </code>
    /// </example>
    public bool DoesDatabaseExist(Guid dbGuid)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        string guid = dbGuid.ToString();
        List<DatabaseInfo> dbList = ListDatabases();
        return dbList.Any(item => item.Id == guid);
    }

    /// <summary>
    /// Asynchronously checks if a database with the specified GUID exists.
    /// </summary>
    /// <param name="dbGuid">The database GUID to check.</param>
    /// <returns>A task that resolves to true if the database exists; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool exists = await admin.DoesDatabaseExistAsync(new Guid("..."));
    /// </code>
    /// </example>
    public async Task<bool> DoesDatabaseExistAsync(Guid dbGuid)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        string guid = dbGuid.ToString();
        List<DatabaseInfo> dbList = await ListDatabasesAsync();
        return dbList.Any(item => item.Id == guid);
    }

    /// <summary>
    /// Creates a new database with the specified name.
    /// </summary>
    /// <param name="databaseName">The name for the new database.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>An IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = admin.CreateDatabase("myDatabase");
    /// </code>
    /// </example>
    public IDatabaseAdmin CreateDatabase(string databaseName, bool waitForDb = true)
    {
        var options = new DatabaseCreationOptions() { Name = databaseName };
        return CreateDatabaseAsync(options, null, waitForDb, true).ResultSync();
    }

    /// <summary>
    /// Creates a new database with the specified creation options.
    /// </summary>
    /// <param name="options">The database creation options.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>An IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = admin.CreateDatabase(new DatabaseCreationOptions { Name = "myDatabase" });
    /// </code>
    /// </example>
    public IDatabaseAdmin CreateDatabase(DatabaseCreationOptions options, bool waitForDb = true)
    {
        return CreateDatabaseAsync(options, null, waitForDb, true).ResultSync();
    }

    /// <summary>
    /// Creates a new database with the specified creation and command options.
    /// </summary>
    /// <param name="options">The database creation options.</param>
    /// <param name="commandOptions">Additional command options.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>An IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = admin.CreateDatabase(new DatabaseCreationOptions { Name = "myDatabase" }, commandOptions);
    /// </code>
    /// </example>
    public IDatabaseAdmin CreateDatabase(DatabaseCreationOptions options, CommandOptions commandOptions, bool waitForDb = true)
    {
        return CreateDatabaseAsync(options, commandOptions, waitForDb, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously creates a new database with the specified name.
    /// </summary>
    /// <param name="databaseName">The name for the new database.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>A task that resolves to an IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = await admin.CreateDatabaseAsync("myDatabase");
    /// </code>
    /// </example>
    public Task<IDatabaseAdmin> CreateDatabaseAsync(string databaseName, bool waitForDb = true)
    {
        var options = new DatabaseCreationOptions() { Name = databaseName };
        return CreateDatabaseAsync(options, null, waitForDb, false);
    }

    /// <summary>
    /// Asynchronously creates a new database with the specified creation options.
    /// </summary>
    /// <param name="creationOptions">The database creation options.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>A task that resolves to an IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = await admin.CreateDatabaseAsync(new DatabaseCreationOptions { Name = "myDatabase" });
    /// </code>
    /// </example>
    public Task<IDatabaseAdmin> CreateDatabaseAsync(DatabaseCreationOptions creationOptions, bool waitForDb = true)
    {
        return CreateDatabaseAsync(creationOptions, null, waitForDb, false);
    }

    /// <summary>
    /// Asynchronously creates a new database with the specified creation and command options.
    /// </summary>
    /// <param name="creationOptions">The database creation options.</param>
    /// <param name="commandOptions">Additional command options.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>A task that resolves to an IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = await admin.CreateDatabaseAsync(new DatabaseCreationOptions { Name = "myDatabase" }, commandOptions);
    /// </code>
    /// </example>
    public Task<IDatabaseAdmin> CreateDatabaseAsync(DatabaseCreationOptions creationOptions, CommandOptions commandOptions, bool waitForDb = true)
    {
        return CreateDatabaseAsync(creationOptions, commandOptions, waitForDb, false);
    }

    internal async Task<IDatabaseAdmin> CreateDatabaseAsync(DatabaseCreationOptions creationOptions, CommandOptions commandOptions, bool waitForDb, bool runSynchronously)
    {
        var databaseName = creationOptions.Name;
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));

        List<DatabaseInfo> dbList = await ListDatabasesAsync(commandOptions, runSynchronously).ConfigureAwait(false);

        DatabaseInfo existingDb = dbList.FirstOrDefault(item => databaseName.Equals(item.Info.Name));

        if (existingDb != null)
        {
            if (existingDb.Status == "ACTIVE")
            {
                Console.WriteLine($"Database {databaseName} already exists and is ACTIVE.");
                return GetDatabaseAdmin(Guid.Parse(existingDb.Id));
            }

            throw new InvalidOperationException($"Database {databaseName} already exists but is in state: {existingDb.Status}");
        }

        Command command = CreateCommand()
            .AddUrlPath("databases")
            .WithPayload(creationOptions)
            .AddCommandOptions(commandOptions);

        Guid newDbId = Guid.Empty;
        command.ResponseHandler = response =>
        {
            if (response.StatusCode == System.Net.HttpStatusCode.Created && response.Headers.TryGetValues("Location", out var values))
            {
                if (Guid.TryParse(values.FirstOrDefault(), out Guid parsedGuid))
                {
                    newDbId = parsedGuid;
                }
            }
            return Task.CompletedTask;
        };
        Command.EmptyResult emptyResult = await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);
        Console.WriteLine($"Database {databaseName} (dbId: {newDbId}) is starting: please wait...");

        if (waitForDb)
        {
            if (runSynchronously)
            {
                WaitForDatabase(databaseName);
            }
            else
            {
                await WaitForDatabaseAsync(databaseName).ConfigureAwait(false);
            }
        }

        return GetDatabaseAdmin(newDbId);
    }

    private void WaitForDatabase(string databaseName)
    {
        WaitForDatabaseAsync(databaseName, true).ResultSync();
    }

    private async Task WaitForDatabaseAsync(string databaseName)
    {
        await WaitForDatabaseAsync(databaseName, false).ConfigureAwait(false);
    }

    internal async Task WaitForDatabaseAsync(string databaseName, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        if (runSynchronously)
        {
            Console.WriteLine($"Waiting {WAIT_IN_SECONDS} seconds synchronously before checking db status...");
            Thread.Sleep(WAIT_IN_SECONDS * 1000);
            string status = GetDatabaseStatus(databaseName);

            if (status != "ACTIVE")
            {
                throw new Exception($"Database {databaseName} is still {status} after {WAIT_IN_SECONDS} seconds.");
            }

            Console.WriteLine($"Database {databaseName} is ready.");
            return;
        }

        const int retry = 30_000; // 30 seconds
        int waiting = 0;

        while (waiting < WAIT_IN_SECONDS * 1000)
        {
            string status = await GetDatabaseStatusAsync(databaseName).ConfigureAwait(false);
            if (status == "ACTIVE")
            {
                Console.WriteLine($"Database {databaseName} is ready.");
                return;
            }

            Console.WriteLine($"Database {databaseName} is {status}... retrying in {retry / 1000} seconds.");
            await Task.Delay(retry).ConfigureAwait(false);
            waiting += retry;
        }

        throw new Exception($"Database {databaseName} did not become ready within {WAIT_IN_SECONDS} seconds.");
    }

    internal string GetDatabaseStatus(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        var db = ListDatabases().FirstOrDefault(item => databaseName.Equals(item.Info.Name));

        if (db == null)
        {
            throw new Exception($"Database '{databaseName}' not found.");
        }

        return db.Status;
    }

    internal async Task<string> GetDatabaseStatusAsync(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        var dbList = await ListDatabasesAsync();
        var db = dbList.FirstOrDefault(item => databaseName.Equals(item.Info.Name));

        if (db == null)
        {
            throw new Exception($"Database '{databaseName}' not found.");
        }

        return db.Status;
    }

    /// <summary>
    /// Drops the database with the specified name.
    /// </summary>
    /// <param name="databaseName">The name of the database to drop.</param>
    /// <returns>True if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = admin.DropDatabase("myDatabase");
    /// </code>
    /// </example>
    public bool DropDatabase(string databaseName)
    {
        return DropDatabaseAsync(databaseName, null, false).ResultSync();
    }

    /// <summary>
    /// Drops the database with the specified GUID.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database to drop.</param>
    /// <returns>True if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = admin.DropDatabase(new Guid("..."));
    /// </code>
    /// </example>
    public bool DropDatabase(Guid dbGuid)
    {
        return DropDatabaseAsync(dbGuid, null, false).ResultSync();
    }

    /// <summary>
    /// Drops the database with the specified name using provided command options.
    /// </summary>
    /// <param name="databaseName">The name of the database to drop.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>True if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = admin.DropDatabase("myDatabase", options);
    /// </code>
    /// </example>
    public bool DropDatabase(string databaseName, CommandOptions options)
    {
        return DropDatabaseAsync(databaseName, options, false).ResultSync();
    }

    /// <summary>
    /// Drops the database with the specified GUID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database to drop.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>True if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = admin.DropDatabase(new Guid("..."), options);
    /// </code>
    /// </example>
    public bool DropDatabase(Guid dbGuid, CommandOptions options)
    {
        return DropDatabaseAsync(dbGuid, options, false).ResultSync();
    }

    /// <summary>
    /// Asynchronously drops the database with the specified name.
    /// </summary>
    /// <param name="databaseName">The name of the database to drop.</param>
    /// <returns>A task that resolves to true if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = await admin.DropDatabaseAsync("myDatabase");
    /// </code>
    /// </example>
    public Task<bool> DropDatabaseAsync(string databaseName)
    {
        return DropDatabaseAsync(databaseName, null, true);
    }

    /// <summary>
    /// Asynchronously drops the database with the specified GUID.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database to drop.</param>
    /// <returns>A task that resolves to true if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = await admin.DropDatabaseAsync(new Guid("..."));
    /// </code>
    /// </example>
    public Task<bool> DropDatabaseAsync(Guid dbGuid)
    {
        return DropDatabaseAsync(dbGuid, null, true);
    }

    /// <summary>
    /// Asynchronously drops the database with the specified name using provided command options.
    /// </summary>
    /// <param name="databaseName">The name of the database to drop.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A task that resolves to true if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = await admin.DropDatabaseAsync("myDatabase", options);
    /// </code>
    /// </example>
    public Task<bool> DropDatabaseAsync(string databaseName, CommandOptions options)
    {
        return DropDatabaseAsync(databaseName, options, true);
    }

    /// <summary>
    /// Asynchronously drops the database with the specified GUID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database to drop.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A task that resolves to true if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = await admin.DropDatabaseAsync(new Guid("..."), options);
    /// </code>
    /// </example>
    public Task<bool> DropDatabaseAsync(Guid dbGuid, CommandOptions options)
    {
        return DropDatabaseAsync(dbGuid, options, true);
    }

    internal async Task<bool> DropDatabaseAsync(string databaseName, CommandOptions options, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        var dbList = await ListDatabasesAsync(options, runSynchronously).ConfigureAwait(false);

        var dbInfo = dbList.FirstOrDefault(item => item.Info.Name.Equals(databaseName));
        if (dbInfo == null)
        {
            return false;
        }

        if (Guid.TryParse(dbInfo.Id, out var dbGuid))
        {
            return await DropDatabaseAsync(dbGuid, options, runSynchronously).ConfigureAwait(false);
        }

        return false;
    }

    internal async Task<bool> DropDatabaseAsync(Guid dbGuid, CommandOptions options, bool runSynchronously)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        var dbInfo = await GetDatabaseInfoAsync(dbGuid, options, runSynchronously).ConfigureAwait(false);
        if (dbInfo != null)
        {
            Command command = CreateCommand()
                .AddUrlPath("databases")
                .AddUrlPath(dbGuid.ToString())
                .AddUrlPath("terminate")
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);

            return true;
        }
        return false;
    }

    private IDatabaseAdmin GetDatabaseAdmin(Guid dbGuid)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        return new DatabaseAdminAstra(dbGuid, _client, null);
    }

    /// <summary>
    /// Retrieves database information for the specified GUID.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database.</param>
    /// <returns>A DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = admin.GetDatabaseInfo(new Guid("..."));
    /// </code>
    /// </example>
    public DatabaseInfo GetDatabaseInfo(Guid dbGuid)
    {
        return GetDatabaseInfoAsync(dbGuid, null, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously retrieves database information for the specified GUID.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database.</param>
    /// <returns>A task that resolves to a DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = await admin.GetDatabaseInfoAsync(new Guid("..."));
    /// </code>
    /// </example>
    public async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid)
    {
        return await GetDatabaseInfoAsync(dbGuid, null, false).ConfigureAwait(false);
    }

    /// <summary>
    /// Retrieves database information for the specified GUID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = admin.GetDatabaseInfo(new Guid("..."), options);
    /// </code>
    /// </example>
    public DatabaseInfo GetDatabaseInfo(Guid dbGuid, CommandOptions options)
    {
        return GetDatabaseInfoAsync(dbGuid, options, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously retrieves database information for the specified GUID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The GUID of the database.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A task that resolves to a DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = await admin.GetDatabaseInfoAsync(new Guid("..."), options);
    /// </code>
    /// </example>
    public async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid, CommandOptions options)
    {
        return await GetDatabaseInfoAsync(dbGuid, options, false).ConfigureAwait(false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid, bool runSynchronously)
    {
        return await GetDatabaseInfoAsync(dbGuid, null, runSynchronously).ConfigureAwait(false);
    }

    internal Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid, CommandOptions options, bool runSynchronously)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        var command = CreateCommand()
            .AddUrlPath("databases")
            .AddUrlPath(dbGuid.ToString())
            .AddCommandOptions(options);

        var response = command.RunAsyncRaw<DatabaseInfo>(HttpMethod.Get, runSynchronously);
        return response;
    }

    private Command CreateCommand()
    {
        return new Command(_client, OptionsTree, new AdminCommandUrlBuilder());
    }
}
