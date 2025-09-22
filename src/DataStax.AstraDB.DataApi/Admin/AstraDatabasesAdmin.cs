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
        return ListDatabases().Select(db => db.Name).ToList();
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
    public Task<List<string>> ListDatabaseNamesAsync()
    {
        return ListDatabaseNamesAsync(null);
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
        return ListDatabases(options).Select(db => db.Name).ToList();
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
        return databases.Select(db => db.Name).ToList();
    }

    /// <summary>
    /// Synchronous version of <see cref="ListDatabasesAsync()"/>
    /// </summary>
    /// <inheritdoc cref="ListDatabasesAsync()"/>
    public List<DatabaseInfo> ListDatabases()
    {
        return ListDatabases(null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListDatabasesAsync(CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="ListDatabasesAsync(CommandOptions)"/>
    public List<DatabaseInfo> ListDatabases(CommandOptions options)
    {
        return ListDatabases(null, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListDatabasesAsync(ListDatabaseOptions)"/>
    /// </summary>
    /// <inheritdoc cref="ListDatabasesAsync(ListDatabaseOptions)"/>
    public List<DatabaseInfo> ListDatabases(ListDatabaseOptions listOptions)
    {
        return ListDatabases(listOptions, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListDatabasesAsync(ListDatabaseOptions, CommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="ListDatabasesAsync(ListDatabaseOptions, CommandOptions)"/>
    public List<DatabaseInfo> ListDatabases(ListDatabaseOptions listOptions, CommandOptions options)
    {
        return ListDatabasesAsync(null, options, true).ResultSync();
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
        return ListDatabasesAsync(null, null);
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
        return ListDatabasesAsync(null, options, false);
    }

    /// <summary>
    ///  Asynchronously returns a list of database info objects using specified filtering options
    /// </summary>
    /// <param name="listOptions"></param>
    /// <returns></returns>
    public Task<List<DatabaseInfo>> ListDatabasesAsync(ListDatabaseOptions listOptions)
    {
        return ListDatabasesAsync(listOptions, null, false);
    }

    /// <summary>
    ///  Asynchronously returns a list of database info objects using specified command options and filtering options
    /// </summary>
    /// <param name="options"></param>
    /// <param name="listOptions"></param>
    /// <returns></returns>
    public Task<List<DatabaseInfo>> ListDatabasesAsync(ListDatabaseOptions listOptions, CommandOptions options)
    {
        return ListDatabasesAsync(listOptions, options, false);
    }

    internal async Task<List<DatabaseInfo>> ListDatabasesAsync(ListDatabaseOptions listOptions, CommandOptions options, bool runSynchronously)
    {
        if (listOptions == null)
        {
            listOptions = new ListDatabaseOptions();
        }

        var command = CreateCommand()
            .AddUrlPath("databases")
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .WithPayload(listOptions)
            .AddCommandOptions(options);

        var rawResults = await command.RunAsyncRaw<List<RawDatabaseInfo>>(HttpMethod.Get, runSynchronously).ConfigureAwait(false);
        return rawResults?.Select(db => new DatabaseInfo(db)).ToList() ?? new List<DatabaseInfo>();
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
        List<string> list = await ListDatabaseNamesAsync().ConfigureAwait(false);
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
        List<DatabaseInfo> dbList = await ListDatabasesAsync().ConfigureAwait(false);
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

        List<DatabaseInfo> dbList = await ListDatabasesAsync(null, commandOptions, runSynchronously).ConfigureAwait(false);

        DatabaseInfo existingDb = dbList.FirstOrDefault(item => databaseName.Equals(item.Name));

        if (existingDb != null)
        {
            if (existingDb.Status == AstraDatabaseStatus.ACTIVE)
            {
                Console.WriteLine($"Database {databaseName} already exists and is ACTIVE.");
                return GetDatabaseAdmin(existingDb);
            }

            throw new InvalidOperationException($"Database {databaseName} already exists but is in state: {existingDb.Status}");
        }

        Command command = CreateCommand()
            .AddUrlPath("databases")
            .WithPayload(creationOptions)
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
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

        return await GetDatabaseAdminAsync(newDbId);
    }

    private void WaitForDatabase(string databaseName)
    {
        WaitForDatabaseAsync(databaseName).ResultSync();
    }

    internal async Task WaitForDatabaseAsync(string databaseName)
    {
        const int MAX_WAIT_IN_SECONDS = 600;
        const int SLEEP_SECONDS = 5;
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));

        int secondsWaited = 0;

        while (secondsWaited < MAX_WAIT_IN_SECONDS)
        {
            var status = await GetDatabaseStatusAsync(databaseName).ConfigureAwait(false);
            if (status == AstraDatabaseStatus.ACTIVE)
            {
                return;
            }
            await Task.Delay(SLEEP_SECONDS * 1000).ConfigureAwait(false);
            secondsWaited += SLEEP_SECONDS;
        }

        throw new Exception($"Database {databaseName} did not become ready within {MAX_WAIT_IN_SECONDS} seconds.");
    }

    internal async Task<AstraDatabaseStatus> GetDatabaseStatusAsync(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        var dbList = await ListDatabasesAsync();
        var db = dbList.FirstOrDefault(item => databaseName.Equals(item.Name));

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
        var dbList = await ListDatabasesAsync(null, options, runSynchronously).ConfigureAwait(false);

        var dbInfo = dbList.FirstOrDefault(item => item.Name.Equals(databaseName));
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
                .WithTimeoutManager(new DatabaseAdminTimeoutManager())
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);

            return true;
        }
        return false;
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
    public Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid)
    {
        return GetDatabaseInfoAsync(dbGuid, null, true);
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
    public Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid, CommandOptions options)
    {
        return GetDatabaseInfoAsync(dbGuid, options, false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid, bool runSynchronously)
    {
        return await GetDatabaseInfoAsync(dbGuid, null, runSynchronously).ConfigureAwait(false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid, CommandOptions options, bool runSynchronously)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        var command = CreateCommand()
            .AddUrlPath("databases")
            .AddUrlPath(dbGuid.ToString())
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddCommandOptions(options);

        var rawInfo = await command.RunAsyncRaw<RawDatabaseInfo>(HttpMethod.Get, runSynchronously);
        var response = new DatabaseInfo(rawInfo);
        return response;
    }

    private DatabaseAdminAstra GetDatabaseAdmin(DatabaseInfo dbInfo)
    {
        var apiEndpoint = $"https://{dbInfo.Id}-{dbInfo.Region}.apps.astra.datastax.com";
        var database = _client.GetDatabase(apiEndpoint);
        return new DatabaseAdminAstra(database, _client, null);
    }

    private async Task<DatabaseAdminAstra> GetDatabaseAdminAsync(Guid dbGuid)
    {
        var dbInfo = await GetDatabaseInfoAsync(dbGuid).ConfigureAwait(false);
        var apiEndpoint = $"https://{dbGuid}-{dbInfo.Region}.apps.astra.datastax.com";
        var database = _client.GetDatabase(apiEndpoint);
        return new DatabaseAdminAstra(database, _client, null);
    }

    private Command CreateCommand()
    {
        return new Command(_client, OptionsTree, new AdminCommandUrlBuilder());
    }
}
