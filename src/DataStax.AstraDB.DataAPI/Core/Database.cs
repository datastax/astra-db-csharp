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

using DataStax.AstraDB.DataAPI.Collections;
using DataStax.AstraDB.DataAPI.Core.Commands;
using DataStax.AstraDB.DataAPI.Utils;

namespace DataStax.AstraDB.DataAPI.Core;

public class Database
{
    private readonly string _apiEndpoint;
    private readonly DatabaseOptions _databaseOptions;
    private readonly DataAPIClient _client;
    private readonly string _urlPostfix = "";

    public string ApiEndpoint => _apiEndpoint;
    internal DatabaseOptions DatabaseOptions => _databaseOptions;
    internal DataAPIClient Client => _client;

    internal Database(string apiEndpoint, DatabaseOptions databaseOptions, DataAPIClient client)
    {
        Guard.NotNullOrEmpty(apiEndpoint, nameof(apiEndpoint));
        Guard.NotNull(databaseOptions, nameof(databaseOptions));
        Guard.NotNull(client, nameof(client));
        _apiEndpoint = apiEndpoint;
        _databaseOptions = databaseOptions;
        _client = client;
    }

    public async Task<Collection<Document>> CreateCollectionAsync(string collectionName)
    {
        var command = CreateCommand("createCollection").WithPayload(new
        {
            name = collectionName
        });
        var response = await command.RunAsync().ConfigureAwait(false);
        //TODO: check for valid response (or part of the runasync)
        // and return error as appropriate
        return GetCollection<Document>(collectionName);
    }

    public Collection<T> GetCollection<T>(string collectionName) where T : class
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        return new Collection<T>(collectionName, this);
    }

    internal Command CreateCommand(string name)
    {
        return new Command(this, _urlPostfix, name);
    }

}
