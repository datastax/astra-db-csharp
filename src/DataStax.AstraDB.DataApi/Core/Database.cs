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
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.Tables;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Entrypoint for the interactions with a specific database such as creating/deleting collections/tables, 
/// connecting to collections/tables, and executing arbitrary commands.
/// 
/// Note that creating an instance of a Database doesn't trigger actual database creation; the database must have already existed beforehand. If you need to create a new database, use the AstraAdmin class.
/// </summary>
/// <remarks>
/// The Database class has a concept of a "current keyspace", which is the keyspace used for all operations. This can be overridden in each method call via an overload with the <see cref="DatabaseCommandOptions"/> parameter,
/// or when creating the <see cref="Database"/> instance (see <see cref="DataAPIClient.GetDatabase(string, DatabaseCommandOptions)"/>).
/// If unset, the default keyspace will be used.
/// </remarks>
/// <example>
/// Using the default keyspace for all operations
/// <code>
/// var client = new DataAPIClient("token");
/// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com");
/// // Check to see if a collection exists in the default keyspace
/// var doesCollectionExist = database.DoesCollectionExist("myCollection");
/// </code>
/// </example>
/// <example>
/// Setting a custom keyspace for all operations
/// <code>
/// var client = new DataAPIClient("token");
/// var dbOptions = new DatabaseCommandOptions() { Keyspace = "myKeyspace" };
/// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com", dbOptions);
/// // Check to see if a collection exists in the custom keyspace
/// var doesCollectionExist = database.DoesCollectionExist("myCollectionInMyKeyspace");
/// </code>
/// </example>
/// <example>
/// Setting a custom keyspace for a single operation
/// <code>
/// var client = new DataAPIClient("token");
/// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com");
/// // Check to see if a collection exists in the custom keyspace
/// var dbOptions = new DatabaseCommandOptions() { Keyspace = "myKeyspace" };
/// var doesCollectionExist = database.DoesCollectionExist("myCollectionInMyKeyspace", dbOptions);
/// </code>
/// </example>
public class Database
{
    internal const string DefaultKeyspace = "default_keyspace";

    private readonly string _apiEndpoint;
    private readonly DataAPIClient _client;
    private readonly string _urlPostfix = "";
    private readonly Guid? _id;

    private DatabaseCommandOptions _dbCommandOptions = new DatabaseCommandOptions();

    /// <summary>
    /// The working keyspace for this database. Unless otherwise specified, this keyspace will be targeted when invoking a method.
    /// </summary>
    public string Keyspace => CommandOptions.Merge(OptionsTree).Keyspace;

    /// <summary>
    /// The database Guid (as a string). If no Guid is known, an empty string is returned.
    /// </summary>
    public string Id => _id == null? "" : _id.ToString();

    internal string APIEndpoint => _apiEndpoint;
    internal DataAPIClient Client => _client;
    internal Guid? DatabaseId => _id;

    internal CommandOptions[] OptionsTree
    {
        get
        {
            return _dbCommandOptions == null ? new CommandOptions[] { _client.ClientOptions } : new CommandOptions[] { _client.ClientOptions, _dbCommandOptions };
        }
    }

    internal Database(string apiEndpoint, DataAPIClient client, DatabaseCommandOptions dbCommandOptions)
    {
        Guard.NotNullOrEmpty(apiEndpoint, nameof(apiEndpoint));
        Guard.NotNull(client, nameof(client));
        _apiEndpoint = apiEndpoint;
        _client = client;
        _dbCommandOptions = dbCommandOptions;
        var maybeId = GetDatabaseIdFromUrl(_apiEndpoint);
        if (maybeId != null)
        {
            _id = (Guid)maybeId;
        }
    }

    /// <summary>
    /// Set the current keyspace to use for all subsequent operations (can be overridden in each method call via an overload with the <see cref="DatabaseCommandOptions"/> parameter)
    /// </summary>
    /// <param name="keyspace"></param>
    public void UseKeyspace(string keyspace)
    {
        if (_dbCommandOptions == null)
        {
            _dbCommandOptions = new DatabaseCommandOptions { Keyspace = keyspace };
        }
        else
        {
            _dbCommandOptions.Keyspace = keyspace;
        }
    }

    ///<summary>
    ///Synchronous version of <see cref="DoesCollectionExistAsync(string)"/>.
    ///</summary>
    /// <inheritdoc cref="DoesCollectionExistAsync(string)"/>
    public bool DoesCollectionExist(string collectionName)
    {
        return DoesCollectionExist(collectionName, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DoesCollectionExistAsync(string, DatabaseCommandOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="DoesCollectionExistAsync(string, DatabaseCommandOptions)"/>
    public bool DoesCollectionExist(string collectionName, DatabaseCommandOptions commandOptions)
    {
        return DoesCollectionExistAsync(collectionName, commandOptions).ResultSync();
    }

    /// <summary>
    /// Looks to see if a collection exists in the database.
    /// </summary>
    /// <param name="collectionName">The name of the collection to check.</param>
    /// <returns>True if the collection exists, false otherwise.</returns>
    public Task<bool> DoesCollectionExistAsync(string collectionName)
    {
        return DoesCollectionExistAsync(collectionName, null);
    }

    /// <inheritdoc cref="DoesCollectionExistAsync(string)" />
    /// <param name="collectionName"></param>
    /// <param name="commandOptions">The options to use for the command, useful for overriding the keyspace.</param>
    public async Task<bool> DoesCollectionExistAsync(string collectionName, DatabaseCommandOptions commandOptions)
    {
        var collectionNames = await ListCollectionsAsync<ListCollectionNamesResult>(includeDetails: false, commandOptions, runSynchronously: false).ConfigureAwait(false);
        return collectionNames.CollectionNames.Count > 0 && collectionNames.CollectionNames.Contains(collectionName);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListCollectionNamesAsync()"/>.
    /// </summary>
    /// <inheritdoc cref="ListCollectionNamesAsync()"/>
    public List<string> ListCollectionNames()
    {
        return ListCollectionNames(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListCollectionNamesAsync(DatabaseCommandOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="ListCollectionNamesAsync(DatabaseCommandOptions)"/>
    public List<string> ListCollectionNames(DatabaseCommandOptions commandOptions)
    {
        return ListCollectionNamesAsync(commandOptions).ResultSync();
    }

    /// <summary>
    /// Get a list of all collection names in the current keyspace.
    /// </summary>
    /// 
    /// <returns>The list of collection names.</returns>
    public Task<List<string>> ListCollectionNamesAsync()
    {
        return ListCollectionNamesAsync(null);
    }

    /// <inheritdoc cref="ListCollectionNamesAsync()" />
    /// <param name="commandOptions">The options to use for the command, useful for overriding the keyspace.</param>
    public async Task<List<string>> ListCollectionNamesAsync(DatabaseCommandOptions commandOptions)
    {
        var result = await ListCollectionsAsync<ListCollectionNamesResult>(includeDetails: false, commandOptions, runSynchronously: false).ConfigureAwait(false);
        return result.CollectionNames;
    }

    /// <summary>
    /// Synchronous version of <see cref="ListCollectionsAsync()"/>.
    /// </summary>
    /// <inheritdoc cref="ListCollectionsAsync()"/>
    public IEnumerable<CollectionInfo> ListCollections()
    {
        return ListCollections(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListCollectionsAsync(DatabaseCommandOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="ListCollectionsAsync(DatabaseCommandOptions)"/>
    public IEnumerable<CollectionInfo> ListCollections(DatabaseCommandOptions commandOptions)
    {
        var result = ListCollectionsAsync<ListCollectionsResult>(includeDetails: true, commandOptions, runSynchronously: true).ResultSync();
        return result.Collections;
    }

    /// <summary>
    /// Get a list of all collections (name and metadata) in the current keyspace.
    /// </summary>
    /// <returns>The list of collections.</returns>
    public Task<IEnumerable<CollectionInfo>> ListCollectionsAsync()
    {
        return ListCollectionsAsync(null);
    }

    /// <inheritdoc cref="ListCollectionsAsync()" />
    /// <param name="commandOptions">The options to use for the command, useful for overriding the keyspace.</param>
    public async Task<IEnumerable<CollectionInfo>> ListCollectionsAsync(DatabaseCommandOptions commandOptions)
    {
        var result = await ListCollectionsAsync<ListCollectionsResult>(true, commandOptions, runSynchronously: false).ConfigureAwait(false);
        return result.Collections;
    }

    private async Task<T> ListCollectionsAsync<T>(bool includeDetails, DatabaseCommandOptions commandOptions, bool runSynchronously)
    {
        object payload = new
        {
            options = new { explain = includeDetails }
        };
        var command = CreateCommand("findCollections")
            .WithPayload(payload)
            .WithTimeoutManager(new CollectionAdminTimeoutManager())
            .AddCommandOptions(commandOptions);
        var response = await command.RunAsyncReturnStatus<T>(runSynchronously).ConfigureAwait(false);
        return response.Result;
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync(string)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync(string)" />
    public Collection<Document> CreateCollection(string collectionName)
    {
        return CreateCollection(collectionName, null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync(string, CreateCollectionOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync(string, CreateCollectionOptions)" />
    public Collection<Document> CreateCollection(string collectionName, CreateCollectionOptions options)
    {
        return CreateCollection(collectionName, null, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync(string, CollectionDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync(string, CollectionDefinition)" />
    public Collection<Document> CreateCollection(string collectionName, CollectionDefinition definition)
    {
        return CreateCollection(collectionName, definition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync(string, CollectionDefinition, CreateCollectionOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync(string, CollectionDefinition, CreateCollectionOptions)" />
    public Collection<Document> CreateCollection(string collectionName, CollectionDefinition definition, CreateCollectionOptions options)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, options, true).ResultSync();
    }

    /// <summary>
    /// Create a new collection in the database, using the keyspace specified in the <see cref="DatabaseCommandOptions"/>
    /// passed to the DataAPIClient's GetDatabase method (for example: <see cref="DataAPIClient.GetDatabase(string, DatabaseCommandOptions)"/>), or the default keyspace otherwise.
    /// </summary>
    /// <param name="collectionName">The name of the collection to create.</param>
    /// <returns>A reference to the created collection.</returns>
    /// <remarks>
    /// This version uses a simple <see cref="Dictionary{String, Object}"/>for the documents stored in the collection
    /// See the <see cref="Document"/> class.
    /// Use the strongly-typed overloads for specify a custom type, for example <see cref="CreateCollectionAsync{T}(string)"/>.
    /// </remarks>
    public Task<Collection<Document>> CreateCollectionAsync(string collectionName)
    {
        return CreateCollectionAsync<Document>(collectionName, null, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync(string)" />
    /// <param name="collectionName"></param>
    /// <param name="options"></param>
    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CreateCollectionOptions options)
    {
        return CreateCollectionAsync<Document>(collectionName, null, options);
    }

    /// <inheritdoc cref="CreateCollectionAsync(string)" />
    /// <param name="collectionName"></param>
    /// <param name="definition">Specify options to use when creating the collection.</param>
    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CollectionDefinition definition)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync(string, CollectionDefinition)" />
    /// <param name="collectionName"></param>
    /// <param name="definition"></param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CollectionDefinition definition, CreateCollectionOptions options)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, options, false);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(string)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    public Collection<T> CreateCollection<T>(string collectionName) where T : class
    {
        return CreateCollection<T>(collectionName, null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(string, CollectionDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition)" />
    public Collection<T> CreateCollection<T>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollection<T>(collectionName, definition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(string, CreateCollectionOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CreateCollectionOptions)" />
    public Collection<T> CreateCollection<T>(string collectionName, CreateCollectionOptions options) where T : class
    {
        return CreateCollection<T>(collectionName, null, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(string, CollectionDefinition, CreateCollectionOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition, CreateCollectionOptions)" />
    public Collection<T> CreateCollection<T>(string collectionName, CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, options, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}()"/>
    /// </summary>
    public Collection<T> CreateCollection<T>() where T : class
    {
        return CreateCollectionAsync<T>(null, null, null, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(CollectionDefinition)"/>
    /// </summary>
    public Collection<T> CreateCollection<T>(CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T>(null, definition, null, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(CreateCollectionOptions)"/>
    /// </summary>
    public Collection<T> CreateCollection<T>(CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T>(null, null, options, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(CollectionDefinition, CreateCollectionOptions)"/>
    /// </summary>
    public Collection<T> CreateCollection<T>(CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T>(null, definition, options, true).ResultSync();
    }

    /// <summary>
    /// Create a new collection in the database, using the keyspace specified in the <see cref="DatabaseCommandOptions"/>
    /// passed to the <see cref="DataAPIClient.GetDatabase(string, DatabaseCommandOptions)"/> method, or the default keyspace otherwise.
    /// </summary>
    /// <typeparam name="T">The type to use for serialization/deserialization of the documents stored in the collection.</typeparam>
    /// <param name="collectionName">The name of the collection to create.</param>
    /// <returns>A reference to the created collection.</returns>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, null, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    /// <param name="collectionName"></param>
    /// <param name="definition">Specify options to use when creating the collection.</param>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    /// <param name="collectionName"></param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, null, options);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition)" />
    /// <param name="collectionName"></param>
    /// <param name="definition"></param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, options, false);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    public Task<Collection<T>> CreateCollectionAsync<T>() where T : class
    {
        return CreateCollectionAsync<T>(null, null, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition)" />
    public Task<Collection<T>> CreateCollectionAsync<T>(CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T>(null, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CreateCollectionOptions)" />
    public Task<Collection<T>> CreateCollectionAsync<T>(CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T>(null, null, options);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition, CreateCollectionOptions)" />
    public Task<Collection<T>> CreateCollectionAsync<T>(CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T>(null, definition, options, false);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T, TId}(string)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T, TId}(string)" />
    public Collection<T, TId> CreateCollection<T, TId>(string collectionName) where T : class
    {
        return CreateCollection<T, TId>(collectionName, null, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T, TId}(string, CollectionDefinition)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T, TId}(string, CollectionDefinition)" />
    public Collection<T, TId> CreateCollection<T, TId>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollection<T, TId>(collectionName, definition, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T, TId}(string, CollectionDefinition, CreateCollectionOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T, TId}(string, CollectionDefinition, CreateCollectionOptions)" />
    public Collection<T, TId> CreateCollection<T, TId>(string collectionName, CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, options, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T, TId}()"/>
    /// </summary>
    public Collection<T, TId> CreateCollection<T, TId>() where T : class
    {
        return CreateCollectionAsync<T, TId>(null, null, null, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T, TId}(CollectionDefinition)"/>
    /// </summary>
    public Collection<T, TId> CreateCollection<T, TId>(CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T, TId>(null, definition, null, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T, TId}(CollectionDefinition, CreateCollectionOptions)"/>
    /// </summary>
    public Collection<T, TId> CreateCollection<T, TId>(CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(null, definition, options, true).ResultSync();
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    /// <typeparam name="T">The type to use for the document.</typeparam>
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, null, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition)" />
    /// <typeparam name="T">The type to use for the document.</typeparam>
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition, CreateCollectionOptions)" />
    /// <typeparam name="T">The type to use for the document.</typeparam>
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, options, false);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    /// <typeparam name="T">The type to use for serialization/deserialization of the documents stored in the collection.</typeparam>
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>() where T : class
    {
        return CreateCollectionAsync<T, TId>(null, null, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition)" />
    /// <typeparam name="T">The type to use for serialization/deserialization of the documents stored in the collection.</typeparam>
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T, TId>(null, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition, CreateCollectionOptions)" />
    /// <typeparam name="T">The type to use for serialization/deserialization of the documents stored in the collection.</typeparam>
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(CollectionDefinition definition, CreateCollectionOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(null, definition, options, false);
    }

    private async Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition, CreateCollectionOptions options, bool runSynchronously) where T : class
    {
        collectionName = MaybeGetCollectionNameFromAttribute<T>(collectionName);
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        await CreateCollectionAsync<T, object>(collectionName, definition, options, runSynchronously);
        return GetCollection<T>(collectionName, options);
    }

    private async Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition, CreateCollectionOptions options, bool runSynchronously) where T : class
    {
        collectionName = MaybeGetCollectionNameFromAttribute<T>(collectionName);
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        if (definition == null)
        {
            definition = CollectionDefinition.Create<T>();
        }
        else
        {
            CollectionDefinition.CheckAddDefinitionsFromAttributes<T>(definition);
        }
        object payload = new
        {
            name = collectionName,
            options = definition
        };
        var command = CreateCommand("createCollection")
            .WithPayload(payload)
            .WithTimeoutManager(new CollectionAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
        return GetCollection<T, TId>(collectionName, options);
    }

    private string MaybeGetCollectionNameFromAttribute<T>(string collectionName)
    {
        if (string.IsNullOrEmpty(collectionName))
        {
            Type type = typeof(T);
            var nameAttribute = type.GetCustomAttribute<CollectionNameAttribute>();
            if (nameAttribute != null)
            {
                collectionName = nameAttribute.Name;
            }
        }
        return collectionName;
    }

    /// <summary>
    /// Returns an instance of <see cref="IDatabaseAdmin"/> that can be used to perform database management operations for this database
    /// </summary>
    /// <param name="options">Optional. The options for the admin, useful e.g. for customizing timeout settings</param>
    /// <returns>An appropriate database admin instance</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the options request a different destination
    /// </exception>
    /// <remarks>
    /// The type-parameterized form of this method, <see cref="GetAdmin{TAdmin}(GetAdminOptions)"/>, is recommended for a more type-safe code.
    /// </remarks>
    public IDatabaseAdmin GetAdmin(GetAdminOptions options = null)
    {
        options ??= new();
        var mergedOptions = CommandOptions.Merge(CommandOptions.Merge(OptionsTree), options);

        if (options is { Destination: not null } && mergedOptions is { Destination: not null } && options.Destination != mergedOptions.Destination)
        {
            throw new ArgumentException("Destination cannot be overridden when supplying additional options.");
        }

        if (mergedOptions.Destination == DataAPIDestination.ASTRA)
        {
            return new DatabaseAdminAstra(this, _client, mergedOptions);
        }
        return new DatabaseAdminDataAPI(this, _client, mergedOptions);
    }

    /// <summary>
    /// Returns an instance of <typeparamref name="TAdmin"/> that can be used to perform database management operations for this database
    /// </summary>
    /// <typeparam name="TAdmin">
    /// The type of database admin to return. Must be either <see cref="DatabaseAdminAstra"/> or <see cref="DatabaseAdminDataAPI"/>
    /// </typeparam>
    /// <param name="options">Optional. The options for the admin, useful e.g. for customizing timeout settings</param>
    /// <returns>An instance of the specified admin type</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the options request a different destination, or when the resulting admin does not match the provided <typeparamref name="TAdmin"/>
    /// </exception>
    public TAdmin GetAdmin<TAdmin>(GetAdminOptions options = null) where TAdmin : IDatabaseAdmin
    {
        IDatabaseAdmin admin = GetAdmin(options);

        // Actual type validation
        if (admin is not TAdmin typedAdmin)
        {
            throw new ArgumentException(
                $"Requested a {typeof(TAdmin).Name}, but produced a {admin.GetType().Name}. " +
                "Please ensure the requested admin type matches the database 'Destination'.");
        }

        return typedAdmin;
    }

    /// <summary>
    /// Returns a reference to the collection with the specified name.
    /// </summary>
    /// <param name="collectionName"></param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<Document> GetCollection(string collectionName, GetCollectionOptions options = null)
    {
        return GetCollection<Document>(collectionName, options);
    }

    /// <summary>
    /// Returns a reference to the collection based on the specified document type. 
    /// The collection name is determined by the <see cref="CollectionNameAttribute"/> on the document type, 
    /// or the name of the document type if the attribute is not present.
    /// </summary>
    /// <typeparam name="T">The type of the document stored in the referenced collection</typeparam>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<T> GetCollection<T>(GetCollectionOptions options = null) where T : class
    {
        return GetCollection<T>(null, options);
    }

    /// <summary>
    /// Returns a reference to the collection with the specified name.
    /// </summary>
    /// <typeparam name="T">The type of the document stored in the referenced collection</typeparam>
    /// <param name="collectionName"></param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<T> GetCollection<T>(string collectionName, GetCollectionOptions options = null) where T : class
    {
        if (string.IsNullOrEmpty(collectionName))
        {
            Type type = typeof(T);
            var nameAttribute = type.GetCustomAttribute<CollectionNameAttribute>();
            if (nameAttribute != null)
            {
                collectionName = nameAttribute.Name;
            }
        }
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        return new Collection<T>(collectionName, this, options);
    }

    /// <summary>
    /// Returns a reference to the collection based on the specified document type. 
    /// The collection name is determined by the <see cref="CollectionNameAttribute"/> on the document type, 
    /// or the name of the document type if the attribute is not present.
    /// </summary>
    /// <typeparam name="T">The type of the document stored in the referenced collection</typeparam>
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<T, TId> GetCollection<T, TId>(GetCollectionOptions options = null) where T : class
    {
        return GetCollection<T, TId>(null, options);
    }

    /// <summary>
    /// Returns a reference to the collection with the specified name.
    /// </summary>
    /// <param name="collectionName"></param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<T, TId> GetCollection<T, TId>(string collectionName, GetCollectionOptions options = null) where T : class
    {
        if (string.IsNullOrEmpty(collectionName))
        {
            Type type = typeof(T);
            var nameAttribute = type.GetCustomAttribute<CollectionNameAttribute>();
            if (nameAttribute != null)
            {
                collectionName = nameAttribute.Name;
            }
        }
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        return new Collection<T, TId>(collectionName, this, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropCollectionAsync(string, DropCollectionOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropCollection(string, DropCollectionOptions)" />
    public void DropCollection(string collectionName, DropCollectionOptions options = null)
    {
        DropCollectionAsync(collectionName, options, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="DropCollectionAsync{T}(DropCollectionOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropCollection(string, DropCollectionOptions)" />
    /// <typeparam name="T">The type of the document stored in the referenced collection</typeparam>
    public void DropCollection<T>(DropCollectionOptions options = null)
    {
        string collectionName = null;
        Type type = typeof(T);
        var nameAttribute = type.GetCustomAttribute<CollectionNameAttribute>();
        if (nameAttribute != null)
        {
            collectionName = nameAttribute.Name;
        }
        DropCollectionAsync(collectionName, options, true).ResultSync();
    }

    /// <summary>
    /// Remove a collection from the database.
    /// </summary>
    /// <param name="collectionName">The collection to remove</param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace</param>
    /// <returns></returns>
    public Task DropCollectionAsync(string collectionName, DropCollectionOptions options = null)
    {
        return DropCollectionAsync(collectionName, options, false);
    }

    /// <summary>
    /// Remove a collection from the database based on the class for its documents.
    /// </summary>
    /// <typeparam name="T">The type of the document stored in the referenced collection. The name of the collection to drop is extracted from this.</typeparam>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace</param>
    /// <returns></returns>
    public Task DropCollectionAsync<T>(DropCollectionOptions options = null)
    {
        string collectionName = null;
        Type type = typeof(T);
        var nameAttribute = type.GetCustomAttribute<CollectionNameAttribute>();
        if (nameAttribute != null)
        {
            collectionName = nameAttribute.Name;
        }
        return DropCollectionAsync(collectionName, options, false);
    }

    private async Task DropCollectionAsync(string collectionName, DropCollectionOptions options, bool runSynchronously)
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        var payload = new
        {
            name = collectionName
        };
        var command = CreateCommand("deleteCollection")
            .WithPayload(payload)
            .WithTimeoutManager(new CollectionAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTableAsync(string, TableDefinition, CreateTableOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTableAsync(string, TableDefinition, CreateTableOptions)" />
    public Table<Row> CreateTable(string tableName, TableDefinition definition, CreateTableOptions options = null)
    {
        if (definition.PrimaryKey == null)
        {
            throw new InvalidOperationException("No primary key defined for table class. Please use definition.AddPrimaryKey() or definition.AddCompoundPrimaryKey()");
        }
        return CreateTableAsync<Row>(tableName, definition, options, true).ResultSync();
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTableAsync{TRow}(CreateTableOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTableAsync{TRow}(CreateTableOptions)" />
    public Table<TRow> CreateTable<TRow>(CreateTableOptions options = null) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        var definition = TableDefinition.CreateTableDefinition<TRow>();
        return CreateTable<TRow>(tableName, definition, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTableAsync{TRow}(string, CreateTableOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTableAsync{TRow}(string, CreateTableOptions)" />
    public Table<TRow> CreateTable<TRow>(string tableName, CreateTableOptions options = null) where TRow : class, new()
    {
        var definition = TableDefinition.CreateTableDefinition<TRow>();
        return CreateTable<TRow>(tableName, definition, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTableAsync{TRow}(TableDefinition, CreateTableOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTableAsync{TRow}(TableDefinition, CreateTableOptions)" />
    public Table<TRow> CreateTable<TRow>(TableDefinition definition, CreateTableOptions options = null) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return CreateTable<TRow>(tableName, definition, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTableAsync{TRow}(string, TableDefinition, CreateTableOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateTableAsync{TRow}(string, TableDefinition, CreateTableOptions)" />
    public Table<TRow> CreateTable<TRow>(string tableName, TableDefinition definition, CreateTableOptions options = null) where TRow : class, new()
    {
        return CreateTableAsync<TRow>(tableName, definition, options, true).ResultSync();
    }

    /// <summary>
    /// Creates a table with the specified name, definition, and options.
    /// </summary>
    /// <param name="tableName">The name to give the table.</param>
    /// <param name="definition">The table definition describing columns and primary key.</param>
    /// <param name="options">Options for the create table command.</param>
    /// <returns>A <see cref="Table{Row}"/> instance for the created table.</returns>
    public Task<Table<Row>> CreateTableAsync(string tableName, TableDefinition definition, CreateTableOptions options = null)
    {
        if (definition.PrimaryKey == null)
        {
            throw new InvalidOperationException("No primary key defined for table class. Please use definition.AddPrimaryKey() or definition.AddCompoundPrimaryKey()");
        }
        return CreateTableAsync<Row>(tableName, definition, options, false);
    }

    /// <summary>
    /// Creates a table using the schema inferred from the <typeparamref name="TRow"/> type.
    /// Any required user-defined type is created as well.
    /// </summary>
    /// <typeparam name="TRow">The type representing the table row schema.</typeparam>
    /// <param name="options">Options for the create table command.</param>
    /// <returns>A <see cref="Table{TRow}"/> instance for the created table.</returns>
    public Task<Table<TRow>> CreateTableAsync<TRow>(CreateTableOptions options = null) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        var definition = TableDefinition.CreateTableDefinition<TRow>();
        return CreateTableAsync<TRow>(tableName, definition, options);
    }

    /// <summary>
    /// Creates a table with the specified name using the schema inferred from the <typeparamref name="TRow"/> type.
    /// Any required user-defined type is created as well.
    /// </summary>
    /// <typeparam name="TRow">The type representing the table row schema.</typeparam>
    /// <param name="tableName">The name to give the table.</param>
    /// <param name="options">Options for the create table command.</param>
    /// <returns>A <see cref="Table{TRow}"/> instance for the created table.</returns>
    public Task<Table<TRow>> CreateTableAsync<TRow>(string tableName, CreateTableOptions options = null) where TRow : class, new()
    {
        var definition = TableDefinition.CreateTableDefinition<TRow>();
        return CreateTableAsync<TRow>(tableName, definition, options, false);
    }

    /// <summary>
    /// Creates a table with a name inferred from the <typeparamref name="TRow"/> type.
    /// Any required user-defined type is created as well.
    /// </summary>
    /// <typeparam name="TRow">The type representing the table row schema.</typeparam>
    /// <param name="definition">A table definition, replacing that inferred from TRow.</param>
    /// <param name="options">Options for the create table command.</param>
    /// <returns>A <see cref="Table{TRow}"/> instance for the created table.</returns>
    public Task<Table<TRow>> CreateTableAsync<TRow>(TableDefinition definition, CreateTableOptions options = null) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return CreateTableAsync<TRow>(tableName, definition, options, false);
    }

    /// <summary>
    /// Creates a table with the specified name and definitions.
    /// Any required user-defined type is created as well.
    /// </summary>
    /// <typeparam name="TRow">The type representing the table row schema.</typeparam>
    /// <param name="tableName">The name to give the table.</param>
    /// <param name="definition">A table definition, replacing that inferred from TRow.</param>
    /// <param name="options">Options for the create table command.</param>
    /// <returns>A <see cref="Table{TRow}"/> instance for the created table.</returns>
    public Task<Table<TRow>> CreateTableAsync<TRow>(string tableName, TableDefinition definition, CreateTableOptions options = null) where TRow : class, new()
    {
        return CreateTableAsync<TRow>(tableName, definition, options, false);
    }

    private async Task<Table<TRow>> CreateTableAsync<TRow>(string tableName, TableDefinition definition, CreateTableOptions options, bool runSynchronously) where TRow : class
    {
        // UDTs
        var udtProperties = TypeUtilities.FindPropertiesWithUserDefinedTypeAttribute(typeof(TRow));
        if (udtProperties.Any())
        {
            var existingTypes = await ListTypesAsync(null, false, runSynchronously).ConfigureAwait(false);
            var existingTypeNames = existingTypes.Select(x => x.Name).ToList();
            foreach (var udtProperty in udtProperties)
            {
                var typeName = UserDefinedTypeRequest.GetUserDefinedTypeName(udtProperty.UnderlyingType, udtProperty.Attribute);
                if (!existingTypeNames.Contains(typeName))
                {
                    await CreateTypeAsync(
                        typeName,
                        UserDefinedTypeRequest.CreateDefinitionFromType(udtProperty.UnderlyingType),
                        new CreateTypeOptions() { IfNotExists = true },
                        runSynchronously
                    ).ConfigureAwait(false);
                }
            }
        }
        // table
        options = options ?? new CreateTableOptions();
        var payload = new CreateTableCommandPayload
        {
            Name = tableName,
            Definition = definition
        };
        if (options.IfNotExists)
        {
            payload.Options = new CreateTableCommandOptions { IfNotExists = options.IfNotExists };
        }
        var command = CreateCommand("createTable")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
        return GetTable<TRow>(tableName, options);
    }

    /// <summary>
    /// Returns a reference to a table by name.
    /// </summary>
    /// <param name="tableName">The name of the table being targeted.</param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    /// <returns>A table class allowing performing operations on the table.</returns>
    public Table<Row> GetTable(string tableName, GetTableOptions options = null)
    {
        return GetTable<Row>(tableName, options);
    }

    /// <summary>
    /// Returns a reference to the table with the name defined by a [TableName] attribute on the row type, or the type name if no attribute is present.
    /// </summary>
    /// <returns>A table class allowing performing operations on the table.</returns>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    /// <typeparam name="TRow">The type of the rows in the table.</typeparam>
    public Table<TRow> GetTable<TRow>(GetTableOptions options = null) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return GetTable<TRow>(tableName, options);
    }

    /// <inheritdoc cref="GetTable{TRow}(GetTableOptions)" />
    /// <param name="tableName">The name of the table - if provided, overrides the information found on the row type.</param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Table<TRow> GetTable<TRow>(string tableName, GetTableOptions options = null) where TRow : class
    {
        options ??= new();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        return new Table<TRow>(tableName, this, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync{TRow}(DropTableOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync{TRow}(DropTableOptions)"/>
    public void DropTable<TRow>(DropTableOptions options = null) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        DropTable(tableName, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync(string, DropTableOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync(string, DropTableOptions)"/>
    public void DropTable(string tableName, DropTableOptions options = null)
    {
        DropTableAsync(tableName, options, true).ResultSync();
    }

    /// <summary>
    /// Drops the table with the name defined by a [TableName] attribute on the TRowtype, or the type name if no attribute is present.
    /// </summary>
    /// <typeparam name="TRow"></typeparam>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Task DropTableAsync<TRow>(DropTableOptions options = null) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return DropTableAsync(tableName, options, false);
    }

    /// <summary>
    /// Drops the table with the specified name.
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Task DropTableAsync(string tableName, DropTableOptions options = null)
    {
        return DropTableAsync(tableName, options, false);
    }

    private async Task DropTableAsync(string tableName, DropTableOptions options, bool runSynchronously)
    {
        var payload = new
        {
            name = tableName,
            options = new
            {
                ifExists = options?.IfExists ?? false
            }
        };
        var command = CreateCommand("dropTable")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTablesAsync()"/>
    /// </summary>
    /// <returns></returns>
    public IEnumerable<TableInfo> ListTables()
    {
        return ListTables(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTablesAsync(DatabaseCommandOptions)"/>
    /// </summary>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    /// <returns></returns>
    public IEnumerable<TableInfo> ListTables(DatabaseCommandOptions options)
    {
        return ListTablesAsync(options, true, true).ResultSync();
    }

    /// <summary>
    /// List the tables in the database.
    /// </summary>
    /// <returns></returns>
    public Task<IEnumerable<TableInfo>> ListTablesAsync()
    {
        return ListTablesAsync(null);
    }

    /// <summary>
    /// List the tables in the database.
    /// </summary>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    /// <returns></returns>
    public Task<IEnumerable<TableInfo>> ListTablesAsync(DatabaseCommandOptions options)
    {
        return ListTablesAsync(options, true, false);
    }

    private async Task<IEnumerable<TableInfo>> ListTablesAsync(DatabaseCommandOptions options, bool includeDetails, bool runSynchronously)
    {
        if (options == null)
        {
            options = new DatabaseCommandOptions();
        }
        options.DeserializeToObjectDictionary = true;
        var payload = new
        {
            options = new
            {
                explain = includeDetails
            }
        };
        var command = CreateCommand("listTables")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        var result = await command.RunAsyncReturnStatus<ListTablesResult>(runSynchronously).ConfigureAwait(false);
        return result.Result.Tables;
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTableNamesAsync()"/>
    /// </summary>
    /// <returns></returns>
    public List<string> ListTableNames()
    {
        return ListTableNames(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTableNamesAsync(DatabaseCommandOptions)"/>
    /// </summary>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    /// <returns></returns>
    public List<string> ListTableNames(DatabaseCommandOptions options)
    {
        return ListTableNamesAsync(options, true, true).ResultSync();
    }

    /// <summary>
    /// List the tables in the database.
    /// </summary>
    /// <returns></returns>
    public Task<List<string>> ListTableNamesAsync()
    {
        return ListTableNamesAsync(null);
    }

    /// <summary>
    /// List the tables in the database.
    /// </summary>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    /// <returns></returns>
    public Task<List<string>> ListTableNamesAsync(DatabaseCommandOptions options)
    {
        return ListTableNamesAsync(options, false, false);
    }

    private async Task<List<string>> ListTableNamesAsync(DatabaseCommandOptions options, bool includeDetails, bool runSynchronously)
    {
        var payload = new
        {
            options = new
            {
                explain = includeDetails
            }
        };
        var command = CreateCommand("listTables")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        var result = await command.RunAsyncReturnStatus<ListTableNamesResult>(runSynchronously).ConfigureAwait(false);
        return result.Result.Tables;
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableIndexAsync(string, DropTableIndexOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableIndexAsync(string, DropTableIndexOptions)"/>
    public void DropTableIndex(string indexName, DropTableIndexOptions options = null)
    {
        DropTableIndexAsync(indexName, options, true).ResultSync();
    }

    /// <summary>
    /// Drops an index on the table.
    /// </summary>
    /// <param name="indexName"></param>
    /// <param name="options"></param>
    public Task DropTableIndexAsync(string indexName, DropTableIndexOptions options = null)
    {
        return DropTableIndexAsync(indexName, options, false);
    }

    private async Task DropTableIndexAsync(string indexName, DropTableIndexOptions options, bool runSynchronously)
    {
        var payload = new Dictionary<string, object>
        {
            ["name"] = indexName,
        };
        if (options != null)
        {
            payload["options"] = new { ifExists = options.IfExists };
        }
        var command = CreateCommand("dropIndex")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);

        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTypeAsync{T}(CreateTypeOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="CreateTypeAsync{T}(CreateTypeOptions)"/>
    public void CreateType<T>(CreateTypeOptions options = null)
    {
        var typeName = UserDefinedTypeRequest.GetUserDefinedTypeName<T>();
        var definition = UserDefinedTypeRequest.CreateDefinitionFromType<T>();
        CreateType(typeName, definition, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTypeAsync{T}(CreateTypeOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="CreateTypeAsync{T}(CreateTypeOptions)"/>
    public void CreateType<T>(string typeName, CreateTypeOptions options = null)
    {
        var definition = UserDefinedTypeRequest.CreateDefinitionFromType<T>();
        CreateType(typeName, definition, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateTypeAsync(string, UserDefinedTypeDefinition, CreateTypeOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="CreateTypeAsync(string, UserDefinedTypeDefinition, CreateTypeOptions)"/>
    public void CreateType(string typeName, UserDefinedTypeDefinition definition, CreateTypeOptions options = null)
    {
        CreateTypeAsync(typeName, definition, options, true).ResultSync();
    }

    /// <summary>
    /// Create a User Defined Type dynamically by specifying the class that defines the type
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="options"></param>
    /// <remarks>
    /// If the class includes a <see cref="UserDefinedTypeAttribute"/> attribute, that name will be used, otherwise the name of the class itself will be used.
    /// Columns that do not match available types (<see cref="TypeUtilities.GetDataAPIType(Type)"/>) will be ignored.
    /// If properties include a <see cref="ColumnNameAttribute"/> attribute, that name will be used, otherwise the name of the property itself will be used. 
    /// </remarks>
    public Task CreateTypeAsync<T>(CreateTypeOptions options = null)
    {
        string typeName = UserDefinedTypeRequest.GetUserDefinedTypeName<T>();
        return CreateTypeAsync<T>(typeName, options);
    }

    /// <summary>
    /// Create a User Defined Type dynamically by specifying the class that defines the type
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="typeName">The name of the type to create (overrides the name extracted from the provided class)</param>
    /// <param name="options"></param>
    /// <remarks>
    /// If the class includes a <see cref="UserDefinedTypeAttribute"/> attribute, that name will be used, otherwise the name of the class itself will be used.
    /// Columns that do not match available types (<see cref="TypeUtilities.GetDataAPIType(Type)"/>) will be ignored.
    /// If properties include a <see cref="ColumnNameAttribute"/> attribute, that name will be used, otherwise the name of the property itself will be used. 
    /// </remarks>
    public Task CreateTypeAsync<T>(string typeName, CreateTypeOptions options = null)
    {
        var definition = UserDefinedTypeRequest.CreateDefinitionFromType<T>();
        return CreateTypeAsync(typeName, definition, options);
    }

    /// <summary>
    /// Create a User Defined Type dynamically by specifying its name and definition
    /// </summary>
    /// <param name="typeName">The name of the type to create</param>
    /// <param name="definition">The type definition</param>
    /// <param name="options"></param>
    public Task CreateTypeAsync(string typeName, UserDefinedTypeDefinition definition, CreateTypeOptions options = null)
    {
        return CreateTypeAsync(typeName, definition, options, false);
    }

    private async Task CreateTypeAsync(string typeName, UserDefinedTypeDefinition definition, CreateTypeOptions options, bool runSynchronously)
    {
        if (options == null)
        {
            options = new CreateTypeOptions();
        }
        var request = new UserDefinedTypeRequest()
        {
            Name = typeName,
            TypeDefinition = definition
        };
        request.SetIfNotExists(options.IfNotExists);
        var command = CreateCommand("createType")
            .WithPayload(request)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTypeAsync{T}(DropTypeOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="DropTypeAsync{T}(DropTypeOptions)"/>
    public void DropType<T>(DropTypeOptions options = null)
    {
        var typeName = UserDefinedTypeRequest.GetUserDefinedTypeName<T>();
        DropType(typeName, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTypeAsync(string, DropTypeOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="DropTypeAsync(string, DropTypeOptions)"/>
    public void DropType(string typeName, DropTypeOptions options = null)
    {
        DropTypeAsync(typeName, options, true).ResultSync();
    }

    /// <summary>
    /// Drop a User Defined Type dynamically by specifying the class that defines the type
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="options"></param>
    /// <remarks>
    /// If the class includes a <see cref="UserDefinedTypeAttribute"/> attribute, that name will be used, otherwise the name of the class itself will be used.
    /// </remarks>
    public Task DropTypeAsync<T>(DropTypeOptions options = null)
    {
        string typeName = UserDefinedTypeRequest.GetUserDefinedTypeName<T>();
        return DropTypeAsync(typeName, options);
    }

    /// <summary>
    /// Drop a User Defined Type dynamically by specifying the type name
    /// </summary>
    /// <param name="typeName">The name of the user-defined type to drop.</param>
    /// <param name="options"></param>
    public Task DropTypeAsync(string typeName, DropTypeOptions options = null)
    {
        return DropTypeAsync(typeName, options, false);
    }

    private async Task DropTypeAsync(string typeName, DropTypeOptions options, bool runSynchronously)
    {
        if (options == null)
        {
            options = new DropTypeOptions();
        }
        var request = new DropUserDefinedTypeRequest()
        {
            Name = typeName
        };
        request.SetIfExists(options.IfExists);
        var command = CreateCommand("dropType")
            .WithPayload(request)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="AlterTypeAsync{T}(IAlterTypeOperation, AlterTypeOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="AlterTypeAsync{T}(IAlterTypeOperation, AlterTypeOptions)"/>
    public void AlterType<T>(IAlterTypeOperation operation, AlterTypeOptions options = null) where T : new()
    {
        var typeName = UserDefinedTypeRequest.GetUserDefinedTypeName<T>();
        AlterType(typeName, operation, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="AlterTypeAsync(string, IAlterTypeOperation, AlterTypeOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="AlterTypeAsync(string, IAlterTypeOperation, AlterTypeOptions)"/>
    public void AlterType(string typeName, IAlterTypeOperation operation, AlterTypeOptions options = null)
    {
        AlterTypeAsync(typeName, operation, options, true).ResultSync();
    }

    /// <summary>
    /// Alter a User Defined Type by specifying the class that defines the type
    /// </summary>
    /// <remarks>
    /// If the class includes a <see cref="UserDefinedTypeAttribute"/> attribute, that name will be used, otherwise the name of the class itself will be used.
    /// </remarks>
    /// <typeparam name="T">The type that defines the User Defined Type</typeparam>
    /// <param name="operation">The operation to apply to the User Defined Type.</param>
    /// <param name="options"></param>
    public Task AlterTypeAsync<T>(IAlterTypeOperation operation, AlterTypeOptions options = null) where T : new()
    {
        var typeName = UserDefinedTypeRequest.GetUserDefinedTypeName<T>();
        return AlterTypeAsync(typeName, operation, options);
    }

    /// <summary>
    /// Alter a User Defined Type given the type name and <see cref="IAlterTypeOperation"/> 
    /// </summary>
    /// <param name="typeName">The name of the User Defined Type to alter.</param>
    /// <param name="operation">The operation to apply to the User Defined Type.</param>
    /// <param name="options"></param>
    public Task AlterTypeAsync(string typeName, IAlterTypeOperation operation, AlterTypeOptions options = null)
    {
        return AlterTypeAsync(typeName, operation, options, false);
    }

    private async Task AlterTypeAsync(string typeName, IAlterTypeOperation operation, AlterTypeOptions options, bool runSynchronously)
    {
        if (options == null)
        {
            options = new AlterTypeOptions();
        }

        var (operationName, operationData) = operation.GetOperation();
        var payload = new Dictionary<string, object>
        {
            ["name"] = typeName,
            [operationName] = operationData
        };

        var command = CreateCommand("alterType")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTypesAsync()"/> 
    /// </summary>
    /// <inheritdoc cref="ListTypesAsync()"/>
    public IEnumerable<string> ListTypeNames()
    {
        return ListTypeNames(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTypesAsync(DatabaseCommandOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="ListTypesAsync(DatabaseCommandOptions)"/>
    public IEnumerable<string> ListTypeNames(DatabaseCommandOptions options)
    {
        return ListTypeNamesAsync(options).ResultSync();
    }

    /// <summary>
    /// List User Defined Types
    /// </summary>
    /// <returns></returns>
    public Task<IEnumerable<string>> ListTypeNamesAsync()
    {
        return ListTypeNamesAsync(null);
    }

    /// <summary>
    /// List User Defined Type names
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    public async Task<IEnumerable<string>> ListTypeNamesAsync(DatabaseCommandOptions options)
    {
        var typeInfos = await ListTypesAsync(options, false, false);
        return typeInfos.Select(x => x.Name);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTypesAsync()"/> 
    /// </summary>
    /// <inheritdoc cref="ListTypesAsync()"/>
    public List<UserDefinedTypeInfo> ListTypes()
    {
        return ListTypes(null);
    }

    /// <summary>
    /// Synchronous version of <see cref="ListTypesAsync(DatabaseCommandOptions)"/> 
    /// </summary>
    /// <inheritdoc cref="ListTypesAsync(DatabaseCommandOptions)"/>
    public List<UserDefinedTypeInfo> ListTypes(DatabaseCommandOptions options)
    {
        return ListTypesAsync(options, true, true).ResultSync();
    }

    /// <summary>
    /// List User Defined Types
    /// </summary>
    /// <returns></returns>
    public Task<List<UserDefinedTypeInfo>> ListTypesAsync()
    {
        return ListTypesAsync(null);
    }

    /// <summary>
    /// List User Defined Types
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    public Task<List<UserDefinedTypeInfo>> ListTypesAsync(DatabaseCommandOptions options)
    {
        return ListTypesAsync(options, true, false);
    }

    private async Task<List<UserDefinedTypeInfo>> ListTypesAsync(DatabaseCommandOptions options, bool includeDetails, bool runSynchronously)
    {
        var payload = new
        {
            options = new
            {
                explain = includeDetails
            }
        };
        var command = CreateCommand("listTypes")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        if (includeDetails)
        {
            var result = await command.RunAsyncReturnStatus<ListUserDefinedTypesResult>(runSynchronously).ConfigureAwait(false);
            return result.Result.Types;
        }
        else
        {
            var result = await command.RunAsyncReturnStatus<ListUserDefinedTypeNamesResult>(runSynchronously).ConfigureAwait(false);
            return result.Result.Types.Select(name => new UserDefinedTypeInfo { Name = name }).ToList();
        }
    }


    internal static Guid? GetDatabaseIdFromUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url))
        {
            return null;
        }
        var match = Regex.Match(url, @"([0-9a-fA-F-]{36})");
        return match.Success ? Guid.Parse(match.Value) : null;
    }

    internal Command CreateCommand(string name)
    {
        return new Command(name, _client, OptionsTree, new DatabaseCommandUrlBuilder(this, _urlPostfix));
    }

}
