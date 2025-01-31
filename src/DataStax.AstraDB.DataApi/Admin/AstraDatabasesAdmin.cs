
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

    public List<DatabaseInfo> ListDatabases()
    {
        throw new NotImplementedException();
    }

    public bool DatabaseExists(string name)
    {
        Guard.NotNullOrEmpty(name, nameof(name));
        return ListDatabaseNames().Contains(name);
    }

    public bool DatabaseExists(Guid id)
    {
        Guard.NotEmpty(id, nameof(id));
        throw new NotImplementedException();
    }

    public IDatabaseAdmin CreateDatabase(string name)
    {
        Guard.NotNullOrEmpty(name, nameof(name));
        throw new NotImplementedException();
        //return CreateDatabase(name, FREE_TIER_CLOUD, FREE_TIER_CLOUD_REGION);
    }

    public IDatabaseAdmin CreateDatabase(string name, CloudProviderType cloudProviderType, string cloudRegion, bool waitForDb = true)
    {
        Guard.NotNullOrEmpty(name, nameof(name));
        Guard.NotNullOrEmpty(cloudRegion, nameof(cloudRegion));

        var dbInfo = ListDatabases().FirstOrDefault(db => name.Equals(db.Info.Name));
        if (dbInfo != null)
        {
            // switch (optDb.Status)
            // {
            //     case DatabaseStatusType.ACTIVE:
            //         Console.WriteLine($"Database {AnsiUtils.Green(name)} already exists and is ACTIVE.");
            //         return GetDatabaseAdmin(Guid.Parse(optDb.Id));
            //     default:
            //         throw new InvalidOperationException("Database already exists but is not in expected state.");
            // }
        }

        // var newDbId = Guid.Parse(devopsDbClient.Create(new DatabaseCreationRequest
        // {
        //     Name = name,
        //     CloudProvider = cloud,
        //     CloudRegion = cloudRegion,
        //     Keyspace = DataApiClientOptions.DEFAULT_KEYSPACE,
        //     WithVector = true
        // }));

        //Console.WriteLine($"Database {name} is starting (id={newDbId}): it will take about a minute please wait...");
        if (waitForDb)
        {
            //WaitForDatabase(devopsDbClient.Database(newDbId.ToString()));
        }
        //return GetDatabaseAdmin(newDbId);
        throw new NotImplementedException();
    }

    public bool DropDatabase(Guid databaseId)
    {
        Guard.NotEmpty(databaseId, nameof(databaseId));
        bool exists = DatabaseExists(databaseId);
        throw new NotImplementedException();
    }

    public bool DropDatabase(string databaseName)
    {
        Guard.NotNullOrEmpty(databaseName, nameof(databaseName));
        var db = ListDatabases().FirstOrDefault(d => d.Info.Name.Equals(databaseName));
        if (db != null)
        {
            throw new NotImplementedException();
            //devopsDbClient.Database(db.Id.ToString()).Delete();
            return true;
        }
        return false;
    }

    public DatabaseInfo GetDatabaseInfo(Guid id)
    {
        return GetDatabaseInfoAsync(id, true).ResultSync();
    }

    public async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid id)
    {
        return await GetDatabaseInfoAsync(id, false).ConfigureAwait(false);
    }

    internal async Task<DatabaseInfo> GetDatabaseInfoAsync(Guid id, bool runSynchronously)
    {
        Guard.NotEmpty(id, nameof(id));
        var command = CreateCommand().AddUrlPath("databases").AddUrlPath(id.ToString());
        var response = await command.RunAsyncRaw<DatabaseInfo>(System.Net.Http.HttpMethod.Get, runSynchronously).ConfigureAwait(false);
        return response;
    }

    private Command CreateCommand()
    {
        return new Command(_client, OptionsTree, new AdminCommandUrlBuilder(OptionsTree));
    }

    //TODO: skip, these are available via DataApiClient
    // public Database GetDatabase(Guid databaseId, DatabaseOptions dbOptions)
    // {
    //     Guard.NotEmpty(databaseId, nameof(databaseId));
    //     throw new NotImplementedException();
    //     // if (!adminOptions.DataApiClientOptions.IsAstra)
    //     // {
    //     //     throw new InvalidEnvironmentException("getDatabase(id, keyspace)", adminOptions.DataApiClientOptions.Destination);
    //     // }

    //     // var databaseRegion = devopsDbClient.FindById(databaseId.ToString()).ValueOr(() => throw new DatabaseNotFoundException(databaseId.ToString())).Info.Region;
    //     // var astraApiEndpoint = new AstraApiEndpoint(databaseId, databaseRegion, adminOptions.DataApiClientOptions.AstraEnvironment);

    //     // return new Database(astraApiEndpoint.ApiEndPoint, dbOptions);
    // }

    //TODO: skip, these are available via DataApiClient
    // public Database GetDatabase(Guid databaseId, string keyspace)
    // {
    //     throw new NotImplementedException();
    //     //return GetDatabase(databaseId, new DatabaseOptions(adminOptions.Token, adminOptions.DataApiClientOptions) { Keyspace = keyspace });
    // }

    //TODO: skip, these are available via DataApiClient
    // public Database GetDatabase(Guid databaseId)
    // {
    //     return GetDatabase(databaseId, DatabaseOptions.DefaultKeyspace);
    // }

    //TODO: is this used? I'd expect to get this from the Database itself.
    // public IDatabaseAdmin GetDatabaseAdmin(Guid databaseId)
    // {
    //     Guard.NotEmpty(databaseId, nameof(databaseId));
    //     throw new NotImplementedException();
    //     //return new DatabaseAdminForAstra(adminOptions.Token, databaseId, adminOptions.DataApiClientOptions);
    // }

    // private void WaitForDatabase(DbOpsClient dbc)
    // {
    //     var top = DateTimeOffset.UtcNow;
    //     while (GetStatus(dbc) != DatabaseStatusType.ACTIVE && (DateTimeOffset.UtcNow - top).TotalSeconds < WAIT_IN_SECONDS)
    //     {
    //         try
    //         {
    //             Thread.Sleep(5000);
    //             Console.WriteLine($"...waiting for database '{dbc.Get().Info.Name}' to become active...");
    //         }
    //         catch (ThreadInterruptedException e)
    //         {
    //             Console.WriteLine($"Interrupted {e.Message}");
    //             Thread.CurrentThread.Interrupt();
    //         }
    //     }
    //     if (GetStatus(dbc) != DatabaseStatusType.ACTIVE)
    //     {
    //         throw new InvalidOperationException($"Database is not in expected state after timeouts of {WAIT_IN_SECONDS} seconds.");
    //     }
    // }

    // private DatabaseStatusType GetStatus(DbOpsClient dbc)
    // {
    //     return dbc.Find().ValueOr(() => throw new DatabaseNotFoundException(dbc.DatabaseId)).Status;
    // }
}

