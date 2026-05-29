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

    internal string DevOpsAPISuffix(DBEnvironment? environment) => environment switch
    {
        DBEnvironment.Production => "apps.astra.datastax.com",
        DBEnvironment.Dev => "apps.astra-dev.datastax.com",
        DBEnvironment.Test => "apps.astra-test.datastax.com",
        _ => "apps.astra.datastax.com"
    };

    /// <summary>
    /// Synchronous version of <see cref="ListDatabaseNamesAsync(ListDatabaseNamesOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="ListDatabaseNamesAsync(ListDatabaseNamesOptions)"/>
    public List<string> ListDatabaseNames(ListDatabaseNamesOptions options = null)
    {
        return ListDatabases(options).Select(db => db.Name).ToList();
    }

    /// <summary>
    /// Returns a list of database names.
    /// </summary>
    /// <param name="options">Options to run the query for databases, including filters.</param>
    /// <returns>A list of the database names.</returns>
    /// <example>
    /// <code>
    /// var names = await admin.ListDatabaseNamesAsync(options);
    /// </code>
    /// </example>
    public async Task<List<string>> ListDatabaseNamesAsync(ListDatabaseNamesOptions options = null)
    {
        var databases = await ListDatabasesAsync(options).ConfigureAwait(false);
        return databases.Select(db => db.Name).ToList();
    }

    /// <summary>
    /// Synchronous version of <see cref="ListDatabasesAsync(ListDatabaseOptions)"/>
    /// </summary>
    /// <inheritdoc cref="ListDatabasesAsync(ListDatabaseOptions)"/>
    public List<DatabaseInfo> ListDatabases(ListDatabaseOptions options = null)
    {
        return ListDatabasesAsync(options, true).ResultSync();
    }

    /// <summary>
    ///  Returns a list of database info objects according to the provided query options.
    /// </summary>
    /// <param name="options">Options to run the query for databases, including filters.</param>
    /// <returns></returns>
    public Task<List<DatabaseInfo>> ListDatabasesAsync(ListDatabaseOptions options = null)
    {
        return ListDatabasesAsync(options, false);
    }

    internal async Task<List<DatabaseInfo>> ListDatabasesAsync(ListDatabaseOptions options, bool runSynchronously)
    {
        if (options == null)
        {
            options = new ListDatabaseOptions();
        }

        var command = CreateCommand()
            .AddUrlPath("databases")
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .WithPayload(options.ToPayload())
            .AddCommandOptions(options);

        var rawResults = await command.RunAsyncRaw<List<RawDatabaseInfo>>(HttpMethod.Get, runSynchronously).ConfigureAwait(false);
        return rawResults?.Select(db => new DatabaseInfo(db)).ToList() ?? new List<DatabaseInfo>();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateDatabaseAsync(CreateDatabaseOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateDatabaseAsync(CreateDatabaseOptions)"/>
    /// <example>
    /// <code>
    /// var adminDb = admin.CreateDatabase(new (){Name = "MyDB", CloudProvider = CloudProviderType.AWS, Region = "us-east-2", waitForCompletion = true});
    /// </code>
    /// </example>
    public DatabaseAdminAstra CreateDatabase(CreateDatabaseOptions options)
    {
        return CreateDatabaseAsync(options, true).ResultSync();
    }

    /// <summary>
    /// Creates a new database according to the provided options.
    /// </summary>
    /// <param name="options">Options for database creation, such as its name, and other settings such as: whether to wait for the DB to become active, timeout settings.</param>
    /// <returns>A DatabaseAdminAstra instance for the created database.</returns>
    /// <example>
    /// <code>
    /// var adminDb = await admin.CreateDatabaseAsync(new (){Name = "MyDB", CloudProvider = CloudProviderType.AWS, Region = "us-east-2", waitForCompletion = true});
    /// </code>
    /// </example>
    /// <remarks>
    /// This method, by default, will wait for the operation to complete on the server side.
    /// Use the options' waitForCompletion attribute to control this behaviour.
    /// </remarks>
    public Task<DatabaseAdminAstra> CreateDatabaseAsync(CreateDatabaseOptions options)
    {
        return CreateDatabaseAsync(options, false);
    }

    internal async Task<DatabaseAdminAstra> CreateDatabaseAsync(CreateDatabaseOptions options, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(options.Name, nameof(options.Name));
        Guard.NotNull(options.CloudProvider, nameof(options.CloudProvider));
        Guard.NotNullOrEmpty(options.Region, nameof(options.Region));
        Command command = CreateCommand()
            .AddUrlPath("databases")
            .WithPayload(options.ToPayload())
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddCommandOptions(options);

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

        if (options.waitForCompletion)
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

        return GetDatabaseAdmin(newDbId, options.Region, GetDatabaseAdminOptions.FromCommandOptions(options));
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
            if (!waitingStatuses.Contains(status))
            {
                throw new Exception($"Database {dbGuid} reached unexpected status {status}");
            }
            await Task.Delay(SLEEP_SECONDS * 1000).ConfigureAwait(false);
            secondsWaited += SLEEP_SECONDS;
        }

        throw new Exception($"Database {dbGuid} did not reach target status {targetStatus} within {MAX_WAIT_IN_SECONDS} seconds.");
    }

    /// <summary>
    /// Synchronous version of <see cref="GetDatabaseStatusAsync(string, GetDatabaseStatusOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="GetDatabaseStatusAsync(string, GetDatabaseStatusOptions)"/>
    public AstraDatabaseStatus GetDatabaseStatus(string dbGuid, GetDatabaseStatusOptions options = null)
    {
        return GetDatabaseStatusAsync(dbGuid, options).ResultSync();
    }

    /// <summary>
    /// Get the status of the database from the DevOps API.
    /// </summary>
    /// <param name="dbGuid">The ID of the target database.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A <see cref="AstraDatabaseStatus"/> value.</returns>
    public async Task<AstraDatabaseStatus> GetDatabaseStatusAsync(string dbGuid, GetDatabaseStatusOptions options = null)
    {
        Guard.NotNullOrEmpty(dbGuid, nameof(dbGuid));

        var dbInfo = await GetDatabaseInfoAsync(dbGuid, options);
        return dbInfo.Status;
    }

    /// <summary>
    /// Synchronous version of <see cref="DropDatabaseAsync(string, DropDatabaseOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="DropDatabaseAsync(string, DropDatabaseOptions)"/>
    /// <example>
    /// <code>
    /// admin.DropDatabase("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    public void DropDatabase(string dbGuid, DropDatabaseOptions options = null)
    {
        DropDatabaseAsync(dbGuid, options, true).ResultSync();
    }

    /// <summary>
    /// Drops the database with the specified ID.
    /// </summary>
    /// <param name="dbGuid">The ID of the database to drop.</param>
    /// <param name="options">The command options to use, including the waitForCompletion flag.</param>
    /// <example>
    /// <code>
    /// await admin.DropDatabaseAsync("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    /// <remarks>
    /// This method, by default, will wait for the operation to complete on the server side.
    /// Use the options' waitForCompletion attribute to control this behaviour.
    /// </remarks>
    public Task DropDatabaseAsync(string dbGuid, DropDatabaseOptions options = null)
    {
        return DropDatabaseAsync(dbGuid, options, false);
    }

    internal async Task DropDatabaseAsync(string dbGuid, DropDatabaseOptions options, bool runSynchronously)
    {
        options ??= new DropDatabaseOptions();
        Guard.NotNullOrEmpty(dbGuid, nameof(dbGuid));
        Command command = CreateCommand()
            .AddUrlPath("databases")
            .AddUrlPath(dbGuid)
            .AddUrlPath("terminate")
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddCommandOptions(options);

        await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);

        if (options.waitForCompletion)
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
    }

    /// <summary>
    /// Returns a DatabaseAdminAstra instance for the database at the specified URL.
    /// </summary>
    /// <param name="apiEndpoint">The API Endpoint for the database, e.g. "https://01234567-89ab-cdef-0123-456789abcdef-us-east1.apps.astra.datastax.com".</param>
    /// <param name="options">Options for the database admin instance.</param>
    /// <returns></returns>
    public DatabaseAdminAstra GetDatabaseAdmin(string apiEndpoint, GetDatabaseAdminOptions options = null)
    {
        var commandOptions = CommandOptions.Merge(new CommandOptions[] {_adminOptions, options});
        var database = _client.GetDatabase(
            apiEndpoint,
            DatabaseCommandOptions.FromCommandOptions(commandOptions)
        );
        return new DatabaseAdminAstra(database, _client, commandOptions);
    }

    /// <summary>
    /// Synchronous version of <see cref="GetDatabaseInfoAsync(string, GetDatabaseInfoOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="GetDatabaseInfoAsync(string, GetDatabaseInfoOptions)"/>
    /// <example>
    /// <code>
    /// var info = admin.GetDatabaseInfo("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    public DatabaseInfo GetDatabaseInfo(string dbGuid, GetDatabaseInfoOptions options = null)
    {
        return GetDatabaseInfoAsync(dbGuid, options, true).ResultSync();
    }

    /// <summary>
    /// Retrieves database information for the specified ID
    /// </summary>
    /// <param name="dbGuid">The ID of the database.</param>
    /// <param name="options">The command options to use.</param>
    /// <returns>A DatabaseInfo object.</returns>
    /// <example>
    /// <code>
    /// var info = await admin.GetDatabaseInfoAsync("a1b2c3d4-e5f6-7890-abcd-ef1234567890", options);
    /// </code>
    /// </example>
    public Task<DatabaseInfo> GetDatabaseInfoAsync(string dbGuid, GetDatabaseInfoOptions options = null)
    {
        return GetDatabaseInfoAsync(dbGuid, options, false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(string dbGuid, GetDatabaseInfoOptions options, bool runSynchronously)
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
    /// Synchronous version of <see cref="FindAvailableRegionsAsync(FindAvailableRegionsOptions)"/>
    /// </summary>
    /// <inheritdoc cref="FindAvailableRegionsAsync(FindAvailableRegionsOptions)"/>
    public List<RegionInfo> FindAvailableRegions(FindAvailableRegionsOptions options = null)
    {
        return FindAvailableRegionsAsync(options, true).ResultSync();
    }

    /// <summary>
    /// Gets a list of available regions for database creation.
    /// </summary>
    /// <param name="options">Additional options to the DevOps API query, such as region filters and general request execution parameters.</param>
    /// <returns>A list of region information matching the provided filters</returns>
    public Task<List<RegionInfo>> FindAvailableRegionsAsync(FindAvailableRegionsOptions options = null)
    {
        return FindAvailableRegionsAsync(options, false);
    }

    internal async Task<List<RegionInfo>> FindAvailableRegionsAsync(FindAvailableRegionsOptions options, bool runSynchronously)
    {
        Dictionary<string, string> parms = new Dictionary<string, string>();
        if (options != null && options.OnlyOrgEnabledRegions.HasValue)
        {
            parms.Add("filter-by-org", options.OnlyOrgEnabledRegions.Value ? "enabled" : "disabled");
        }
        var command = CreateCommand()
            .AddUrlPath("regions/serverless")
            .WithTimeoutManager(new DatabaseAdminTimeoutManager())
            .AddQueryParameters(parms)
            .AddCommandOptions(options);

        var response = await command.RunAsyncRaw<List<RegionInfo>>(HttpMethod.Get, runSynchronously);
        return response;
    }

    private DatabaseAdminAstra GetDatabaseAdmin(DatabaseInfo dbInfo, GetDatabaseAdminOptions options = null)
    {
        var commandOptions = CommandOptions.Merge(new CommandOptions[] {_adminOptions, options});
        var apiEndpoint = $"https://{dbInfo.Id}-{dbInfo.Region}.{DevOpsAPISuffix(commandOptions.Environment)}";
        return GetDatabaseAdmin(
            apiEndpoint,
            GetDatabaseAdminOptions.FromCommandOptions(commandOptions)
        );
    }

    private DatabaseAdminAstra GetDatabaseAdmin(string dbGuid, string region, GetDatabaseAdminOptions options = null)
    {
        var commandOptions = CommandOptions.Merge(new CommandOptions[] {_adminOptions, options});
        var apiEndpoint = $"https://{dbGuid}-{region}.{DevOpsAPISuffix(commandOptions.Environment)}";
        return GetDatabaseAdmin(
            apiEndpoint,
            GetDatabaseAdminOptions.FromCommandOptions(commandOptions)
        );
    }

    private static readonly CommandOptions _devOpsAPIOptions = new CommandOptions { SerializeDateAsDollarDate = false };

    private Command CreateCommand()
    {
        var options = OptionsTree.Concat(new[] { _devOpsAPIOptions }).ToArray();
        return new Command(_client, options, new AdminCommandUrlBuilder());
    }
}
