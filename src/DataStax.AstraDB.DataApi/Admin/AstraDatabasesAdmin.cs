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
    private readonly DataAPIClient _client;

    private CommandOptions[] OptionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };

    private static readonly HashSet<AstraDatabaseStatus> _creatingDatabaseStatuses = new HashSet<AstraDatabaseStatus>
    {
        AstraDatabaseStatus.ASSOCIATING,
        AstraDatabaseStatus.INITIALIZING,
        AstraDatabaseStatus.PENDING
    };
    
    private static readonly HashSet<AstraDatabaseStatus> _droppingDatabaseStatuses = new HashSet<AstraDatabaseStatus>
    {
        AstraDatabaseStatus.TERMINATING
    };

    internal AstraDatabasesAdmin(DataAPIClient client, CommandOptions adminOptions)
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
    /// Checks if a database with the specified name or ID exists.
    /// </summary>
    /// <param name="databaseNameOrId">The database name or ID to check. If the string matches either a database's name or ID, it will be considered a match.</param>
    /// <returns>True if the database exists (by ID or name); otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool exists = admin.DoesDatabaseExist("myDatabase");
    /// // or by ID
    /// bool exists = admin.DoesDatabaseExist("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    /// </code>
    /// </example>
    public bool DoesDatabaseExist(string databaseNameOrId)
    {
        Guard.NotNullOrEmpty(databaseNameOrId, nameof(databaseNameOrId));

        var databases = ListDatabases();
        return databases.Any(item => item.Id == databaseNameOrId || item.Name == databaseNameOrId);
    }

    /// <summary>
    /// Asynchronously checks if a database with the specified name or ID exists.
    /// </summary>
    /// <param name="databaseNameOrId">The database name or ID to check. If the string matches either a database's name or ID, it will be considered a match.</param>
    /// <returns>A task that resolves to true if the database exists (by ID or name); otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool exists = await admin.DoesDatabaseExistAsync("myDatabase");
    /// // or by ID
    /// bool exists = await admin.DoesDatabaseExistAsync("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    /// </code>
    /// </example>
    public async Task<bool> DoesDatabaseExistAsync(string databaseNameOrId)
    {
        Guard.NotNullOrEmpty(databaseNameOrId, nameof(databaseNameOrId));
        
        var databases = await ListDatabasesAsync().ConfigureAwait(false);
        return databases.Any(item => item.Id == databaseNameOrId || item.Name == databaseNameOrId);
    }

    /// <summary>
    /// Creates a new database with the specified creation options.
    /// </summary>
    /// <param name="options">The database creation options.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>An IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = admin.CreateDatabase(new (){Name="MyDB", CloudProvider=CloudProviderType.AWS, Region="us-east-2"});
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
    /// var adminDb = admin.CreateDatabase(new (){Name="MyDB", CloudProvider=CloudProviderType.AWS, Region="us-east-2"}, commandOptions);
    /// </code>
    /// </example>
    public IDatabaseAdmin CreateDatabase(DatabaseCreationOptions options, CommandOptions commandOptions, bool waitForDb = true)
    {
        return CreateDatabaseAsync(options, commandOptions, waitForDb, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously creates a new database with the specified creation options.
    /// </summary>
    /// <param name="creationOptions">The database creation options.</param>
    /// <param name="waitForDb">Whether to wait until the database becomes active.</param>
    /// <returns>A task that resolves to an IDatabaseAdmin instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = await admin.CreateDatabaseAsync(new (){Name="MyDB", CloudProvider=CloudProviderType.AWS, Region="us-east-2"});
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
    /// var adminDb = await admin.CreateDatabaseAsync(new (){Name="MyDB", CloudProvider=CloudProviderType.AWS, Region="us-east-2"}, commandOptions);
    /// </code>
    /// </example>
    public Task<IDatabaseAdmin> CreateDatabaseAsync(DatabaseCreationOptions creationOptions, CommandOptions commandOptions, bool waitForDb = true)
    {
        return CreateDatabaseAsync(creationOptions, commandOptions, waitForDb, false);
    }

    internal async Task<IDatabaseAdmin> CreateDatabaseAsync(DatabaseCreationOptions creationOptions, CommandOptions commandOptions, bool waitForDb, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(creationOptions.Name, nameof(creationOptions.Name));
        Guard.NotNull(creationOptions.CloudProvider, nameof(creationOptions.CloudProvider));
        Guard.NotNullOrEmpty(creationOptions.Region, nameof(creationOptions.Region));
        Command command = CreateCommand()
            .AddUrlPath("databases")
            .WithPayload(creationOptions)
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddCommandOptions(commandOptions);

        var newDbId = "";
        command.ResponseHandler = response =>
        {
            if (response.StatusCode == System.Net.HttpStatusCode.Created && response.Headers.TryGetValues("Location", out var values))
            {
                newDbId = values.FirstOrDefault();
            }
            return Task.CompletedTask;
        };
        await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);

        if (waitForDb)
        {
            if (runSynchronously)
            {
                WaitForDatabase(newDbId, _creatingDatabaseStatuses, AstraDatabaseStatus.ACTIVE);
            }
            else
            {
                await WaitForDatabaseAsync(newDbId, _creatingDatabaseStatuses, AstraDatabaseStatus.ACTIVE).ConfigureAwait(false);
            }
        }

        return await GetDatabaseAdminAsync(newDbId, creationOptions.Region);
    }

    private void WaitForDatabase(string dbGuid, HashSet<AstraDatabaseStatus> waitingStatuses, AstraDatabaseStatus targetStatus)
    {
        WaitForDatabaseAsync(dbGuid, waitingStatuses, targetStatus).ResultSync();
    }

    internal async Task WaitForDatabaseAsync(string dbGuid, HashSet<AstraDatabaseStatus> waitingStatuses, AstraDatabaseStatus targetStatus)
    {
        const int MAX_WAIT_IN_SECONDS = 600;
        const int SLEEP_SECONDS = 5;
        Guard.NotNullOrEmpty(dbGuid, nameof(dbGuid));

        int secondsWaited = 0;

        while (secondsWaited < MAX_WAIT_IN_SECONDS)
        {
            var status = await GetDatabaseStatusAsync(dbGuid).ConfigureAwait(false);
            if (status == targetStatus)
            {
                return;
            }
            if(!waitingStatuses.Contains(status)){
                throw new Exception($"Database {dbGuid} reached unexpected status {status}");
            }
            await Task.Delay(SLEEP_SECONDS * 1000).ConfigureAwait(false);
            secondsWaited += SLEEP_SECONDS;
        }

        throw new Exception($"Database {dbGuid} did not reach target status {targetStatus} within {MAX_WAIT_IN_SECONDS} seconds.");
    }

    internal async Task<AstraDatabaseStatus> GetDatabaseStatusAsync(string dbGuid)
    {
        Guard.NotNullOrEmpty(dbGuid, nameof(dbGuid));

        var dbInfo = await GetDatabaseInfoAsync(dbGuid);
        return dbInfo.Status;
    }

    /// <summary>
    /// Drops the database with the specified ID.
    /// </summary>
    /// <param name="dbGuid">The ID of the database to drop.</param>
    /// <param name="waitForDb">Whether to wait until the database is terminated.</param>
    /// <returns>True if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = admin.DropDatabase("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    /// </code>
    /// </example>
    public bool DropDatabase(string dbGuid, bool waitForDb = true)
    {
        return DropDatabaseAsync(dbGuid, null, waitForDb, false).ResultSync();
    }

    /// <summary>
    /// Drops the database with the specified ID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The ID of the database to drop.</param>
    /// <param name="options">The command options to use.</param>
    /// <param name="waitForDb">Whether to wait until the database is terminated.</param>
    /// <returns>True if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = admin.DropDatabase("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    public bool DropDatabase(string dbGuid, CommandOptions options, bool waitForDb = true)
    {
        return DropDatabaseAsync(dbGuid, options, waitForDb, false).ResultSync();
    }

    /// <summary>
    /// Asynchronously drops the database with the specified ID.
    /// </summary>
    /// <param name="dbGuid">The ID of the database to drop.</param>
    /// <param name="waitForDb">Whether to wait until the database is terminated.</param>
    /// <returns>A task that resolves to true if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = await admin.DropDatabaseAsync("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    /// </code>
    /// </example>
    public Task<bool> DropDatabaseAsync(string dbGuid, bool waitForDb = true)
    {
        return DropDatabaseAsync(dbGuid, null, waitForDb, true);
    }

    /// <summary>
    /// Asynchronously drops the database with the specified ID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The ID of the database to drop.</param>
    /// <param name="options">The command options to use.</param>
    /// <param name="waitForDb">Whether to wait until the database is terminated.</param>
    /// <returns>A task that resolves to true if the database was dropped successfully; otherwise, false.</returns>
    /// <example>
    /// <code>
    /// bool dropped = await admin.DropDatabaseAsync("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    public Task<bool> DropDatabaseAsync(string dbGuid, CommandOptions options, bool waitForDb = true)
    {
        return DropDatabaseAsync(dbGuid, options, waitForDb, true);
    }

    internal async Task<bool> DropDatabaseAsync(string dbGuid, CommandOptions options, bool waitForDb, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(dbGuid, nameof(dbGuid));
        Command command = CreateCommand()
            .AddUrlPath("databases")
            .AddUrlPath(dbGuid)
            .AddUrlPath("terminate")
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddCommandOptions(options);

        await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);

        if (waitForDb)
        {
            if (runSynchronously)
            {
                WaitForDatabase(dbGuid, _droppingDatabaseStatuses, AstraDatabaseStatus.TERMINATED);
            }
            else
            {
                await WaitForDatabaseAsync(dbGuid, _droppingDatabaseStatuses, AstraDatabaseStatus.TERMINATED).ConfigureAwait(false);
            }
        }

        return true;
    }

    /// <summary>
    /// Returns an IDatabaseAdmin instance for the database at the specified URL.
    /// </summary>
    /// <param name="dbUrl"></param>
    /// <returns></returns>
    public IDatabaseAdmin GetDatabaseAdmin(string dbUrl)
    {
        var database = _client.GetDatabase(dbUrl);
        return new DatabaseAdminAstra(database, _client, _adminOptions);
    }

    /// <summary>
    /// Retrieves database information for the specified ID.
    /// </summary>
    /// <param name="dbGuid">The ID of the database.</param>
    /// <returns>A DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = admin.GetDatabaseInfo("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    /// </code>
    /// </example>
    public DatabaseInfo GetDatabaseInfo(string dbGuid)
    {
        return GetDatabaseInfoAsync(dbGuid, null, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously retrieves database information for the specified ID.
    /// </summary>
    /// <param name="dbGuid">The ID of the database.</param>
    /// <returns>A task that resolves to a DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = await admin.GetDatabaseInfoAsync("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    /// </code>
    /// </example>
    public Task<DatabaseInfo> GetDatabaseInfoAsync(string dbGuid)
    {
        return GetDatabaseInfoAsync(dbGuid, null, true);
    }

    /// <summary>
    /// Retrieves database information for the specified ID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The ID of the database.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = admin.GetDatabaseInfo("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    public DatabaseInfo GetDatabaseInfo(string dbGuid, CommandOptions options)
    {
        return GetDatabaseInfoAsync(dbGuid, options, true).ResultSync();
    }

    /// <summary>
    /// Asynchronously retrieves database information for the specified ID using provided command options.
    /// </summary>
    /// <param name="dbGuid">The ID of the database.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A task that resolves to a DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = await admin.GetDatabaseInfoAsync("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    public Task<DatabaseInfo> GetDatabaseInfoAsync(string dbGuid, CommandOptions options)
    {
        return GetDatabaseInfoAsync(dbGuid, options, false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(string dbGuid, bool runSynchronously)
    {
        return await GetDatabaseInfoAsync(dbGuid, null, runSynchronously).ConfigureAwait(false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(string dbGuid, CommandOptions options, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(dbGuid, nameof(dbGuid));
        var command = CreateCommand()
            .AddUrlPath("databases")
            .AddUrlPath(dbGuid)
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddCommandOptions(options);

        var rawInfo = await command.RunAsyncRaw<RawDatabaseInfo>(HttpMethod.Get, runSynchronously);
        var response = new DatabaseInfo(rawInfo);
        return response;
    }

    /// <summary>
    /// Synchronous version of <see cref="FindAvailableRegionsAsync()"/>
    /// </summary>
    /// <returns></returns>
    public List<RegionInfo> FindAvailableRegions()
    {
        return FindAvailableRegions(new FindAvailableRegionsCommandOptions());
    }

    /// <summary>
    /// Synchronous version of <see cref="FindAvailableRegionsAsync(FindAvailableRegionsCommandOptions)"/>
    /// </summary>
    /// <returns></returns>
    public List<RegionInfo> FindAvailableRegions(FindAvailableRegionsCommandOptions options)
    {
        return FindAvailableRegionsAsync(options, true).ResultSync();
    }

    /// <summary>
    /// Gets a list of available regions for database creation.
    /// </summary>
    /// <returns></returns>
    public Task<List<RegionInfo>> FindAvailableRegionsAsync()
    {
        return FindAvailableRegionsAsync(new FindAvailableRegionsCommandOptions());
    }

    /// <summary>
    /// Gets a list of available regions for database creation.
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    public Task<List<RegionInfo>> FindAvailableRegionsAsync(FindAvailableRegionsCommandOptions options)
    {
        return FindAvailableRegionsAsync(options, false);
    }

    internal async Task<List<RegionInfo>> FindAvailableRegionsAsync(FindAvailableRegionsCommandOptions options, bool runSynchronously)
    {
        Dictionary<string, string> parms = new Dictionary<string, string>();
        string regionType = "";
        switch (options.RegionType)
        {
            case RegionTypeFilter.All:
                regionType = "all";
                break;
            case RegionTypeFilter.Serverless:
                regionType = "";
                break;
            case RegionTypeFilter.Vector:
                regionType = "vector";
                break;
        }
        parms.Add("region-type", regionType);
        parms.Add("filter-by-org", options.OnlyOrgEnabledRegions ? "enabled" : "disabled");
        var command = CreateCommand()
            .AddUrlPath("regions/serverless")
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddQueryParameters(parms)
            .AddCommandOptions(options);

        var response = await command.RunAsyncRaw<List<RegionInfo>>(HttpMethod.Get, runSynchronously);
        return response;
    }

    private DatabaseAdminAstra GetDatabaseAdmin(DatabaseInfo dbInfo)
    {
        var apiEndpoint = $"https://{dbInfo.Id}-{dbInfo.Region}.apps.astra.datastax.com";
        var database = _client.GetDatabase(apiEndpoint);
        return new DatabaseAdminAstra(database, _client, _adminOptions);
    }

    private async Task<DatabaseAdminAstra> GetDatabaseAdminAsync(string dbGuid, string region)
    {
        var apiEndpoint = $"https://{dbGuid}-{region}.apps.astra.datastax.com";
        var database = _client.GetDatabase(apiEndpoint);
        return new DatabaseAdminAstra(database, _client, _adminOptions);
    }

    private Command CreateCommand()
    {
        return new Command(_client, OptionsTree, new AdminCommandUrlBuilder());
    }
}
