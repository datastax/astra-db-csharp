
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

    public List<string> ListDatabaseNames()
    {
        return ListDatabases().Select(db => db.Info.Name).ToList();
    }

    public async Task<List<string>> ListDatabaseNamesAsync()
    {
        var databases = await ListDatabasesAsync().ConfigureAwait(false);
        return databases.Select(db => db.Info.Name).ToList();
    }

    public List<string> ListDatabaseNames(CommandOptions options)
    {
        return ListDatabases(options).Select(db => db.Info.Name).ToList();
    }

    public async Task<List<string>> ListDatabaseNamesAsync(CommandOptions options)
    {
        var databases = await ListDatabasesAsync(options).ConfigureAwait(false);
        return databases.Select(db => db.Info.Name).ToList();
    }

    public List<DatabaseInfo> ListDatabases()
    {
        return ListDatabasesAsync(null, true).ResultSync();
    }

    public Task<List<DatabaseInfo>> ListDatabasesAsync()
    {
        return ListDatabasesAsync(null, false);
    }

    public List<DatabaseInfo> ListDatabases(CommandOptions options)
    {
        return ListDatabasesAsync(options, true).ResultSync();
    }

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

    public bool DoesDatabaseExist(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        List<string> list = ListDatabaseNames();
        return list.Contains(databaseName);
    }

    public async Task<bool> DoesDatabaseExistAsync(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        List<string> list = await ListDatabaseNamesAsync();
        return list.Contains(databaseName);
    }

    public bool DoesDatabaseExist(Guid dbGuid)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        string guid = dbGuid.ToString();
        List<DatabaseInfo> dbList = ListDatabases();
        return dbList.Any(item => item.Id == guid);
    }

    public async Task<bool> DoesDatabaseExistAsync(Guid dbGuid)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        string guid = dbGuid.ToString();
        List<DatabaseInfo> dbList = await ListDatabasesAsync();
        return dbList.Any(item => item.Id == guid);
    }

    public IDatabaseAdmin CreateDatabase(string databaseName, bool waitForDb = true)
    {
        var options = new DatabaseCreationOptions() { Name = databaseName };
        return CreateDatabaseAsync(options, null, waitForDb, true).ResultSync();
    }

    public IDatabaseAdmin CreateDatabase(DatabaseCreationOptions options, bool waitForDb = true)
    {
        return CreateDatabaseAsync(options, null, waitForDb, true).ResultSync();
    }

    public IDatabaseAdmin CreateDatabase(DatabaseCreationOptions options, CommandOptions commandOptions, bool waitForDb = true)
    {
        return CreateDatabaseAsync(options, commandOptions, waitForDb, true).ResultSync();
    }

    public Task<IDatabaseAdmin> CreateDatabaseAsync(string databaseName, bool waitForDb = true)
    {
        var options = new DatabaseCreationOptions() { Name = databaseName };
        return CreateDatabaseAsync(options, null, waitForDb, false);
    }

    public Task<IDatabaseAdmin> CreateDatabaseAsync(DatabaseCreationOptions creationOptions, bool waitForDb = true)
    {
        return CreateDatabaseAsync(creationOptions, null, waitForDb, false);
    }

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

    public bool DropDatabase(string databaseName)
    {
        return DropDatabaseAsync(databaseName, null, false).ResultSync();
    }

    public bool DropDatabase(Guid dbGuid)
    {
        return DropDatabaseAsync(dbGuid, null, false).ResultSync();
    }

    public bool DropDatabase(string databaseName, CommandOptions options)
    {
        return DropDatabaseAsync(databaseName, options, false).ResultSync();
    }

    public bool DropDatabase(Guid dbGuid, CommandOptions options)
    {
        return DropDatabaseAsync(dbGuid, options, false).ResultSync();
    }

    public Task<bool> DropDatabaseAsync(string databaseName)
    {
        return DropDatabaseAsync(databaseName, null, true);
    }

    public Task<bool> DropDatabaseAsync(Guid dbGuid)
    {
        return DropDatabaseAsync(dbGuid, null, true);
    }

    public Task<bool> DropDatabaseAsync(string databaseName, CommandOptions options)
    {
        return DropDatabaseAsync(databaseName, options, true);
    }

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

    public DatabaseInfo GetDatabaseInfo(Guid dbGuid)
    {
        return GetDatabaseInfoAsync(dbGuid, null, true).ResultSync();
    }

    public async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid)
    {
        return await GetDatabaseInfoAsync(dbGuid, null, false).ConfigureAwait(false);
    }

    public DatabaseInfo GetDatabaseInfo(Guid dbGuid, CommandOptions options)
    {
        return GetDatabaseInfoAsync(dbGuid, options, true).ResultSync();
    }

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