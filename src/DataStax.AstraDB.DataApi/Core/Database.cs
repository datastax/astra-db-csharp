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
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.Utils;
using System.Collections;
using System.Collections.Generic;
using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core;

public class Database
{
    public const string DefaultKeyspace = "default_keyspace";

    private readonly string _apiEndpoint;
    private readonly DataApiClient _client;
    private readonly string _urlPostfix = "";
    private readonly DatabaseOptions _dbCommandOptions;
    private readonly Guid _id;

    public string ApiEndpoint => _apiEndpoint;
    internal DataApiClient Client => _client;

    internal Guid DatabaseId => _id;

    internal CommandOptions[] OptionsTree
    {
        get
        {
            return _dbCommandOptions == null ? new CommandOptions[] { _client.ClientOptions } : new CommandOptions[] { _client.ClientOptions, _dbCommandOptions };
        }
    }

    internal Database(string apiEndpoint, DataApiClient client, DatabaseOptions dbCommandOptions)
    {
        Guard.NotNullOrEmpty(apiEndpoint, nameof(apiEndpoint));
        Guard.NotNull(client, nameof(client));
        _apiEndpoint = apiEndpoint;
        _client = client;
        _dbCommandOptions = dbCommandOptions;
        _id = (Guid)GetDatabaseIdFromUrl(_apiEndpoint);
    }

    public bool DoesCollectionExist(string collectionName)
    {
        return DoesCollectionExist(collectionName, null);
    }

    public bool DoesCollectionExist(string collectionName, CommandOptions commandOptions)
    {
        return DoesCollectionExistAsync(collectionName, commandOptions).ResultSync();
    }

    public Task<bool> DoesCollectionExistAsync(string collectionName)
    {
        return DoesCollectionExistAsync(collectionName, null);
    }

    public async Task<bool> DoesCollectionExistAsync(string collectionName, CommandOptions commandOptions)
    {
        var collectionNames = await ListCollectionsAsync<ListCollectionNamesResult>(includeDetails: false, commandOptions, runSynchronously: false).ConfigureAwait(false);
        return collectionNames.CollectionNames.Count > 0 && collectionNames.CollectionNames.Contains(collectionName);
    }

    public List<string> ListCollectionNames()
    {
        return ListCollectionNames(null);
    }

    public List<string> ListCollectionNames(CommandOptions commandOptions)
    {
        return ListCollectionNamesAsync(commandOptions).ResultSync();
    }

    public Task<List<string>> ListCollectionNamesAsync()
    {
        return ListCollectionNamesAsync(null);
    }

    public async Task<List<string>> ListCollectionNamesAsync(CommandOptions commandOptions)
    {
        var result = await ListCollectionsAsync<ListCollectionNamesResult>(includeDetails: false, commandOptions, runSynchronously: false).ConfigureAwait(false);
        return result.CollectionNames;
    }

    public IEnumerable<CollectionInfo> ListCollections()
    {
        return ListCollections(null);
    }

    public IEnumerable<CollectionInfo> ListCollections(CommandOptions commandOptions)
    {
        var result = ListCollectionsAsync<ListCollectionsResult>(includeDetails: true, commandOptions, runSynchronously: true).ResultSync();
        return result.Collections;
    }

    public Task<IEnumerable<CollectionInfo>> ListCollectionsAsync()
    {
        return ListCollectionsAsync(null);
    }

    public async Task<IEnumerable<CollectionInfo>> ListCollectionsAsync(CommandOptions commandOptions)
    {
        var result = await ListCollectionsAsync<ListCollectionsResult>(true, commandOptions, runSynchronously: false).ConfigureAwait(false);
        return result.Collections;
    }

    private async Task<T> ListCollectionsAsync<T>(bool includeDetails, CommandOptions commandOptions, bool runSynchronously)
    {
        object payload = new
        {
            options = new { explain = includeDetails }
        };
        var command = CreateCommand("findCollections").WithPayload(payload).AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<T>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    public Collection<Document> CreateCollection(string collectionName)
    {
        return CreateCollection(collectionName, null, null);
    }

    public Collection<Document> CreateCollection(string collectionName, CommandOptions options)
    {
        return CreateCollection(collectionName, null, options);
    }

    public Collection<Document> CreateCollection(string collectionName, CollectionDefinition definition)
    {
        return CreateCollection(collectionName, definition, null);
    }

    public Collection<Document> CreateCollection(string collectionName, CollectionDefinition definition, CommandOptions options)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, options, false).ResultSync();
    }

    public Task<Collection<Document>> CreateCollectionAsync(string collectionName)
    {
        return CreateCollectionAsync<Document>(collectionName, null, null);
    }

    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CommandOptions commandOptions)
    {
        return CreateCollectionAsync<Document>(collectionName, null, commandOptions);
    }

    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CollectionDefinition definition)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, null);
    }

    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CollectionDefinition definition, CommandOptions options)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, options, false);
    }

    public Collection<T> CreateCollection<T>(string collectionName) where T : class
    {
        //TODO: when creating a collection without defining the id type, we need to check the type of the id property or throw an error if it doesn't match the default
        //ie ... currently if the id property is of type int, a guid is still created
        return CreateCollection<T>(collectionName, null, null);
    }

    public Collection<T> CreateCollection<T>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollection<T>(collectionName, definition, null);
    }

    public Collection<T> CreateCollection<T>(string collectionName, CollectionDefinition definition, CommandOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, options, false).ResultSync();
    }

    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, null, null);
    }

    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, null);
    }

    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition, CommandOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, options, false);
    }

    public Collection<T, TId> CreateCollection<T, TId>(string collectionName) where T : class
    {
        return CreateCollection<T, TId>(collectionName, null, null);
    }

    public Collection<T, TId> CreateCollection<T, TId>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollection<T, TId>(collectionName, definition, null);
    }

    public Collection<T, TId> CreateCollection<T, TId>(string collectionName, CollectionDefinition definition, CommandOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, options, false).ResultSync();
    }

    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, null, null);
    }

    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, null);
    }

    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition, CommandOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, options, false);
    }

    private async Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition, CommandOptions options, bool runSynchronously) where T : class
    {
        object payload = definition == null ? new
        {
            name = collectionName
        } : new
        {
            name = collectionName,
            options = definition
        };
        var command = CreateCommand("createCollection").WithPayload(payload).AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
        return GetCollection<T>(collectionName);
    }

    private async Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition, CommandOptions options, bool runSynchronously) where T : class
    {
        object payload = definition == null ? new
        {
            name = collectionName
        } : new
        {
            name = collectionName,
            options = definition
        };
        var command = CreateCommand("createCollection").WithPayload(payload).AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
        return GetCollection<T, TId>(collectionName);
    }

    public Collection<Document> GetCollection(string collectionName)
    {
        return GetCollection(collectionName, new CommandOptions());
    }

    public Collection<Document> GetCollection(string collectionName, CommandOptions options)
    {
        return GetCollection<Document>(collectionName, options);
    }

    public Collection<T> GetCollection<T>(string collectionName) where T : class
    {
        return GetCollection<T>(collectionName, new CommandOptions());
    }

    public Collection<T> GetCollection<T>(string collectionName, CommandOptions options) where T : class
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        return new Collection<T>(collectionName, this, options);
    }

    public Collection<T, TId> GetCollection<T, TId>(string collectionName) where T : class
    {
        return GetCollection<T, TId>(collectionName, new CommandOptions());
    }

    public Collection<T, TId> GetCollection<T, TId>(string collectionName, CommandOptions options) where T : class
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        return new Collection<T, TId>(collectionName, this, options);
    }

    public void DropCollection(string collectionName)
    {
        DropCollection(collectionName, null);
    }

    public void DropCollection(string collectionName, CommandOptions options)
    {
        DropCollectionAsync(collectionName, options, true).ResultSync();
    }

    public Task DropCollectionAsync(string collectionName)
    {
        return DropCollectionAsync(collectionName, new CommandOptions());
    }

    public Task DropCollectionAsync(string collectionName, CommandOptions options)
    {
        return DropCollectionAsync(collectionName, options, false);
    }

    private async Task DropCollectionAsync(string collectionName, CommandOptions options, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        var command = CreateCommand("deleteCollection").WithPayload(new
        {
            name = collectionName
        }).AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
    }

    public static Guid? GetDatabaseIdFromUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url))
            return null;

        // Match the first UUID in the URL
        var match = Regex.Match(url, @"([0-9a-fA-F-]{36})");
        return match.Success ? Guid.Parse(match.Value) : null;
    }

    internal Command CreateCommand(string name)
    {
        return new Command(name, _client, OptionsTree, new DatabaseCommandUrlBuilder(this, _urlPostfix));
    }

}
