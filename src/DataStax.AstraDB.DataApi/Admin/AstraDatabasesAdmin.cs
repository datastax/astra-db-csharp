
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
using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Admin;

public class AstraDatabasesAdmin
{
    private const int WAIT_IN_SECONDS = 600;
    private const CloudProviderType FREE_TIER_CLOUD = CloudProviderType.GCP;
    private const string FREE_TIER_CLOUD_REGION = "us-east1";

    private readonly CommandOptions _adminOptions;
    private readonly DataApiClient _client;

    private CommandOptions[] OptionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };

    internal AstraDatabasesAdmin(DataApiClient client, CommandOptions adminOptions)
    {
        Guard.NotNull(client, nameof(client));
        _client = client;
        Guard.NotNull(adminOptions, nameof(adminOptions));
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

    public List<DatabaseInfo> ListDatabases()
    {
        return ListDatabasesAsync(true).ResultSync();
    }

    public async Task<List<DatabaseInfo>> ListDatabasesAsync()
    {
        return await ListDatabasesAsync(false).ConfigureAwait(false);
    }

    internal async Task<List<DatabaseInfo>> ListDatabasesAsync(bool runSynchronously)
    {
        var command = CreateCommand().AddUrlPath("databases");
        var response = await command.RunAsyncRaw<List<DatabaseInfo>>(HttpMethod.Get, runSynchronously).ConfigureAwait(false);
        return response;
    }

    public bool DoesDatabaseExist(string dbName)
    {
        Guard.NotNullOrEmpty(dbName, nameof(dbName));
        List<string> list = ListDatabaseNames();
        return list.Contains(dbName);
    }

    public async Task<bool> DoesDatabaseExistAsync(string dbName)
    {
        Guard.NotNullOrEmpty(dbName, nameof(dbName));
        List<string> list = await ListDatabaseNamesAsync();
        return list.Contains(dbName);
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

    public IDatabaseAdmin CreateDatabase(string dbName, bool waitForDb = true)
    {
        return CreateDatabaseAsync(dbName, FREE_TIER_CLOUD, FREE_TIER_CLOUD_REGION, waitForDb, true).ResultSync();
    }

    public IDatabaseAdmin CreateDatabase(string dbName, CloudProviderType cloudProviderType, string cloudRegion, bool waitForDb = true)
    {
        return CreateDatabaseAsync(dbName, cloudProviderType, cloudRegion, waitForDb, true).ResultSync();
    }

    public async Task<IDatabaseAdmin> CreateDatabaseAsync(string dbName, bool waitForDb = true)
    {
        return await CreateDatabaseAsync(dbName, FREE_TIER_CLOUD, FREE_TIER_CLOUD_REGION, waitForDb, false).ConfigureAwait(false);
    }

    public async Task<IDatabaseAdmin> CreateDatabaseAsync(string dbName, CloudProviderType cloudProviderType, string cloudRegion, bool waitForDb = true)
    {
        return await CreateDatabaseAsync(dbName, cloudProviderType, cloudRegion, waitForDb, false).ConfigureAwait(false);
    }

    internal async Task<IDatabaseAdmin> CreateDatabaseAsync(string dbName, CloudProviderType cloudProviderType, string cloudRegion, bool waitForDb, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(dbName, nameof(dbName));
        Guard.NotNullOrEmpty(cloudRegion, nameof(cloudRegion));

        List<DatabaseInfo> dbList = await ListDatabasesAsync(runSynchronously).ConfigureAwait(false);

        DatabaseInfo existingDb = dbList.FirstOrDefault(item => dbName.Equals(item.Info.Name));

        if (existingDb != null)
        {
            if (existingDb.Status == "ACTIVE")
            {
                Console.WriteLine($"Database {dbName} already exists and is ACTIVE.");
                return GetDatabaseAdmin(Guid.Parse(existingDb.Id));
            }

            throw new InvalidOperationException($"Database {dbName} already exists but is in state: {existingDb.Status}");
        }

        var requestBody = new
        {
            dbName = dbName,
            cloudProvider = cloudProviderType.ToString(),
            region = cloudRegion,
            keyspace = "default_keyspace",
            capacityUnits = 1,
            tier = "serverless",
        };

        Command command = CreateCommand()
            .AddUrlPath("databases")
            .WithPayload(requestBody);

        // Astra returns the ID of the created DB in the Location header of a response with status code 201 (Created).
        // Here we define a method called by Command.RunAsyncRaw (just before deserialization) to capture that dbId.
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
        };
        Command.EmptyResult emptyResult = await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);
        Console.WriteLine($"Database {dbName} (dbId: {newDbId}) is starting: please wait...");

        if (waitForDb)
        {
            if (runSynchronously)
            {
                WaitForDatabase(dbName);
            }
            else
            {
                await WaitForDatabaseAsync(dbName).ConfigureAwait(false);
            }
        }

        return GetDatabaseAdmin(newDbId);
    }

    private void WaitForDatabase(string dbName)
    {
        WaitForDatabaseAsync(dbName, true);
    }

    private async Task WaitForDatabaseAsync(string dbName)
    {
        await WaitForDatabaseAsync(dbName, false).ConfigureAwait(false);
    }

    internal async Task WaitForDatabaseAsync(string dbName, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(dbName, nameof(dbName));
        if (runSynchronously)
        {
            Console.WriteLine($"Waiting {WAIT_IN_SECONDS} seconds synchronously before checking db status...");
            Thread.Sleep(WAIT_IN_SECONDS * 1000);
            string status = GetDatabaseStatus(dbName);

            if (status != "ACTIVE")
            {
                throw new Exception($"Database {dbName} is still {status} after {WAIT_IN_SECONDS} seconds.");
            }

            Console.WriteLine($"Database {dbName} is ready.");
            return;
        }

        const int retry = 30_000; // 30 seconds
        int waiting = 0;

        while (waiting < WAIT_IN_SECONDS * 1000)
        {
            string status = await GetDatabaseStatusAsync(dbName).ConfigureAwait(false);
            if (status == "ACTIVE")
            {
                Console.WriteLine($"Database {dbName} is ready.");
                return;
            }

            Console.WriteLine($"Database {dbName} is {status}... retrying in {retry / 1000} seconds.");
            await Task.Delay(retry).ConfigureAwait(false);
            waiting += retry;
        }

        throw new Exception($"Database {dbName} did not become ready within {WAIT_IN_SECONDS} seconds.");
    }

    internal string GetDatabaseStatus(string dbName)
    {
        Guard.NotNullOrEmpty(dbName, nameof(dbName));
        var db = ListDatabases().FirstOrDefault(item => dbName.Equals(item.Info.Name));

        if (db == null)
        {
            throw new Exception($"Database '{dbName}' not found.");
        }

        return db.Status;
    }

    internal async Task<string> GetDatabaseStatusAsync(string dbName)
    {
        Guard.NotNullOrEmpty(dbName, nameof(dbName));
        var dbList = await ListDatabasesAsync();
        var db = dbList.FirstOrDefault(item => dbName.Equals(item.Info.Name));

        if (db == null)
        {
            throw new Exception($"Database '{dbName}' not found.");
        }

        return db.Status;
    }

    public bool DropDatabase(string dbName)
    {
        return DropDatabaseAsync(dbName, false).ResultSync();
    }

    public bool DropDatabase(Guid dbGuid)
    {
        return DropDatabaseAsync(dbGuid, false).ResultSync();
    }

    public async Task<bool> DropDatabaseAsync(string dbName)
    {
        return await DropDatabaseAsync(dbName, true).ConfigureAwait(false);
    }

    public async Task<bool> DropDatabaseAsync(Guid dbGuid)
    {
        return await DropDatabaseAsync(dbGuid, true).ConfigureAwait(false);
    }

    internal async Task<bool> DropDatabaseAsync(string dbName, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(dbName, nameof(dbName));
        var dbList = await ListDatabasesAsync(runSynchronously).ConfigureAwait(false);

        var dbInfo = dbList.FirstOrDefault(item => item.Info.Name.Equals(dbName));
        if (dbInfo == null)
        {
            return false;
        }

        if (Guid.TryParse(dbInfo.Id, out var dbGuid))
        {
            return await DropDatabaseAsync(dbGuid, runSynchronously).ConfigureAwait(false);
        }

        return false;
    }

    internal async Task<bool> DropDatabaseAsync(Guid dbGuid, bool runSynchronously)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        var dbInfo = await GetDatabaseInfoAsync(dbGuid, runSynchronously).ConfigureAwait(false);
        if (dbInfo != null)
        {
            Command command = CreateCommand()
                .AddUrlPath("databases")
                .AddUrlPath(dbGuid.ToString())
                .AddUrlPath("terminate");

            Command.EmptyResult emptyResult = await command.RunAsyncRaw<Command.EmptyResult>(runSynchronously).ConfigureAwait(false);

            return true;
        }
        return false;
    }

    private IDatabaseAdmin GetDatabaseAdmin(Guid dbGuid)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        return new DatabaseAdminAstra(dbGuid);
    }

    public DatabaseInfo GetDatabaseInfo(Guid dbGuid)
    {
        return GetDatabaseInfoAsync(dbGuid, true).ResultSync();
    }

    public async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid)
    {
        return await GetDatabaseInfoAsync(dbGuid, false).ConfigureAwait(false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid dbGuid, bool runSynchronously)
    {
        Guard.NotEmpty(dbGuid, nameof(dbGuid));
        var command = CreateCommand().AddUrlPath("databases").AddUrlPath(dbGuid.ToString());
        var response = await command.RunAsyncRaw<DatabaseInfo>(HttpMethod.Get, runSynchronously).ConfigureAwait(false);
        return response;
    }

    private Command CreateCommand()
    {
        return new Command(_client, OptionsTree, new AdminCommandUrlBuilder(OptionsTree));
    }
}