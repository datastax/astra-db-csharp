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

using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Utils;

namespace DataStax.AstraDB.DataApi.Core;

public class Database
{
    private readonly string _apiEndpoint;
    private readonly DatabaseOptions _databaseOptions;
    private readonly DataApiClient _client;
    private readonly string _urlPostfix = "";
    private readonly CommandOptions _dbCommandOptions;

    public string ApiEndpoint => _apiEndpoint;
    internal DatabaseOptions DatabaseOptions => _databaseOptions;
    internal DataApiClient Client => _client;

    internal CommandOptions[] OptionsTree
    {
        get
        {
            return new CommandOptions[] { _client.ClientOptions, _dbCommandOptions };
        }
    }

    //TODO: is DatabaseOptions necessary? Perhaps override CommandOptions.
    internal Database(string apiEndpoint, DataApiClient client, CommandOptions dbCommandOptions, DatabaseOptions databaseOptions)
    {
        Guard.NotNullOrEmpty(apiEndpoint, nameof(apiEndpoint));
        Guard.NotNull(databaseOptions, nameof(databaseOptions));
        Guard.NotNull(client, nameof(client));
        Guard.NotNull(dbCommandOptions, nameof(dbCommandOptions));
        _apiEndpoint = apiEndpoint;
        _databaseOptions = databaseOptions;
        _client = client;
        _dbCommandOptions = dbCommandOptions;
    }

    //TODO: make sync version
    public async Task<Collection<Document>> CreateCollectionAsync(string collectionName)
    {
        var command = CreateCommand("createCollection").WithPayload(new
        {
            name = collectionName
        });
        var response = await command.RunAsync(false).ConfigureAwait(false);
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
        return new Command(name, _client, OptionsTree, new DatabaseCommandUrlBuilder(this, OptionsTree, _urlPostfix));
    }

}
