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
/// or when creating the <see cref="Database"/> instance (see <see cref="DataApiClient.GetDatabase(string, DatabaseCommandOptions)"/>).
/// If unset, the default keyspace will be used.
/// </remarks>
/// <example>
/// Using the default keyspace for all operations
/// <code>
/// var client = new DataApiClient("token");
/// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com");
/// // Check to see if a collection exists in the default keyspace
/// var doesCollectionExist = database.DoesCollectionExist("myCollection");
/// </code>
/// </example>
/// <example>
/// Setting a custom keyspace for all operations
/// <code>
/// var client = new DataApiClient("token");
/// var dbOptions = new DatabaseCommandOptions() { Keyspace = "myKeyspace" };
/// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com", dbOptions);
/// // Check to see if a collection exists in the custom keyspace
/// var doesCollectionExist = database.DoesCollectionExist("myCollectionInMyKeyspace");
/// </code>
/// </example>
/// <example>
/// Setting a custom keyspace for a single operation
/// <code>
/// var client = new DataApiClient("token");
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
    private readonly DataApiClient _client;
    private readonly string _urlPostfix = "";
    private readonly Guid _id;

    private DatabaseCommandOptions _dbCommandOptions;

    internal string ApiEndpoint => _apiEndpoint;
    internal DataApiClient Client => _client;
    internal Guid DatabaseId => _id;

    internal CommandOptions[] OptionsTree
    {
        get
        {
            return _dbCommandOptions == null ? new CommandOptions[] { _client.ClientOptions } : new CommandOptions[] { _client.ClientOptions, _dbCommandOptions };
        }
    }

    internal Database(string apiEndpoint, DataApiClient client, DatabaseCommandOptions dbCommandOptions)
    {
        Guard.NotNullOrEmpty(apiEndpoint, nameof(apiEndpoint));
        Guard.NotNull(client, nameof(client));
        _apiEndpoint = apiEndpoint;
        _client = client;
        _dbCommandOptions = dbCommandOptions;
        _id = (Guid)GetDatabaseIdFromUrl(_apiEndpoint);
    }

    /// <summary>
    /// Set the current keyspace to use for all subsequent operations (can be overridden in each method call via an overload with the <see cref="DatabaseCommandOptions"/> parameter)
    /// </summary>
    /// <param name="keyspace"></param>
    public void UseKeyspace(string keyspace)
    {
        var commandOptions = _dbCommandOptions ?? new DatabaseCommandOptions();
        commandOptions.Keyspace = keyspace;
        _dbCommandOptions = commandOptions;
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
    /// Synchronous version of <see cref="CreateCollectionAsync(string, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync(string, DatabaseCommandOptions)" />
    public Collection<Document> CreateCollection(string collectionName, DatabaseCommandOptions options)
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
    /// Synchronous version of <see cref="CreateCollectionAsync(string, CollectionDefinition, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync(string, CollectionDefinition, DatabaseCommandOptions)" />
    public Collection<Document> CreateCollection(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, options, false).ResultSync();
    }

    /// <summary>
    /// Create a new collection in the database, using the keyspace specified in the <see cref="DatabaseCommandOptions"/>
    /// passed to the DataApiClient's GetDatabase method (for example: <see cref="DataApiClient.GetDatabase(string, DatabaseCommandOptions)"/>), or the default keyspace otherwise.
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
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, DatabaseCommandOptions commandOptions)
    {
        return CreateCollectionAsync<Document>(collectionName, null, commandOptions);
    }

    /// <inheritdoc cref="CreateCollectionAsync(string)" />
    /// <param name="definition">Specify options to use when creating the collection.</param>
    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CollectionDefinition definition)
    {
        return CreateCollectionAsync<Document>(collectionName, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync(string, CollectionDefinition)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Task<Collection<Document>> CreateCollectionAsync(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options)
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
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(string, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T}(string, DatabaseCommandOptions)" />
    public Collection<T> CreateCollection<T>(string collectionName, DatabaseCommandOptions options) where T : class
    {
        return CreateCollection<T>(collectionName, null, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="CreateCollectionAsync{T}(string, CollectionDefinition, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition, DatabaseCommandOptions)" />
    public Collection<T> CreateCollection<T>(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, options, false).ResultSync();
    }

    /// <summary>
    /// Create a new collection in the database, using the keyspace specified in the <see cref="DatabaseCommandOptions"/>
    /// passed to the <see cref="DataApiClient.GetDatabase(string, DatabaseCommandOptions)"/> method, or the default keyspace otherwise.
    /// </summary>
    /// <typeparam name="T">The type to use for serialization/deserialization of the documents stored in the collection.</typeparam>
    /// <param name="collectionName">The name of the collection to create.</param>
    /// <returns>A reference to the created collection.</returns>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, null, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    /// <param name="definition">Specify options to use when creating the collection.</param>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, DatabaseCommandOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, null, options);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options) where T : class
    {
        return CreateCollectionAsync<T>(collectionName, definition, options, false);
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
    /// Synchronous version of <see cref="CreateCollectionAsync{T, TId}(string, CollectionDefinition, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateCollectionAsync{T, TId}(string, CollectionDefinition, DatabaseCommandOptions)" />
    public Collection<T, TId> CreateCollection<T, TId>(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, options, false).ResultSync();
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string)" />
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, null, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition)" />
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, null);
    }

    /// <inheritdoc cref="CreateCollectionAsync{T}(string, CollectionDefinition, DatabaseCommandOptions)" />
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options) where T : class
    {
        return CreateCollectionAsync<T, TId>(collectionName, definition, options, false);
    }

    private async Task<Collection<T>> CreateCollectionAsync<T>(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options, bool runSynchronously) where T : class
    {
        await CreateCollectionAsync<T, object>(collectionName, definition, options, runSynchronously);
        return GetCollection<T>(collectionName);
    }

    private async Task<Collection<T, TId>> CreateCollectionAsync<T, TId>(string collectionName, CollectionDefinition definition, DatabaseCommandOptions options, bool runSynchronously) where T : class
    {
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
        return GetCollection<T, TId>(collectionName);
    }

    /// <summary>
    /// Returns an instance of <see cref="IDatabaseAdmin"/> that can be used to perform database management operations for this database
    /// </summary>
    /// <returns></returns>
    public IDatabaseAdmin GetAdmin()
    {
        return GetAdmin(null);
    }

    /// <summary>
    /// Returns an instance of <see cref="IDatabaseAdmin"/> that can be used to perform database management operations for this database
    /// </summary>
    /// <param name="options">The options to use for the command, useful for overriding the destination database</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException">Thrown when the destination is not the same for all CommandOptions when overriding the default destination</exception>
    public IDatabaseAdmin GetAdmin(CommandOptions options)
    {
        var baseCommandOptions = CommandOptions.Merge(OptionsTree);
        if (options != null && options.Destination != null && baseCommandOptions != null && baseCommandOptions.Destination != null && options.Destination != baseCommandOptions.Destination)
        {
            throw new ArgumentException("Destination must be the same for all CommandOptions when overriding the default destination");
        }
        var destination = options != null && options.Destination != null ? options.Destination :
            baseCommandOptions == null ? DataApiDestination.ASTRA : baseCommandOptions.Destination;
        if (destination == DataApiDestination.ASTRA)
        {
            return new DatabaseAdminAstra(this, _client, options);
        }
        return new DatabaseAdminOther(this, _client, options);
    }

    /// <summary>
    /// Returns a reference to the collection with the specified name.
    /// </summary>
    /// <param name="collectionName"></param>
    /// <returns></returns>
    public Collection<Document> GetCollection(string collectionName)
    {
        return GetCollection(collectionName, null);
    }

    /// <inheritdoc cref="GetCollection(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<Document> GetCollection(string collectionName, DatabaseCommandOptions options)
    {
        return GetCollection<Document>(collectionName, options);
    }

    /// <inheritdoc cref="GetCollection(string)" />
    /// <typeparam name="T">The type of the document stored in the referenced collection</typeparam>
    public Collection<T> GetCollection<T>(string collectionName) where T : class
    {
        return GetCollection<T>(collectionName, null);
    }

    /// <inheritdoc cref="GetCollection{T}(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<T> GetCollection<T>(string collectionName, DatabaseCommandOptions options) where T : class
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        return new Collection<T>(collectionName, this, options);
    }

    /// <inheritdoc cref="GetCollection{T}(string)" />
    /// <typeparam name="TId">The type to use for the document id.</typeparam>
    public Collection<T, TId> GetCollection<T, TId>(string collectionName) where T : class
    {
        return GetCollection<T, TId>(collectionName, null);
    }

    /// <inheritdoc cref="GetCollection{T, TId}(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace.</param>
    public Collection<T, TId> GetCollection<T, TId>(string collectionName, DatabaseCommandOptions options) where T : class
    {
        Guard.NotNullOrEmpty(collectionName, nameof(collectionName));
        return new Collection<T, TId>(collectionName, this, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropCollectionAsync(string)"/>
    /// </summary>
    /// <inheritdoc cref="DropCollection(string)" />
    public void DropCollection(string collectionName)
    {
        DropCollection(collectionName, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropCollectionAsync(string, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropCollection(string, DatabaseCommandOptions)" />
    public void DropCollection(string collectionName, DatabaseCommandOptions options)
    {
        DropCollectionAsync(collectionName, options, true).ResultSync();
    }

    /// <summary>
    /// Remove a collection from the database.
    /// </summary>
    /// <param name="collectionName">The collection to remove</param>
    /// <returns></returns>
    public Task DropCollectionAsync(string collectionName)
    {
        return DropCollectionAsync(collectionName, null);
    }

    /// <inheritdoc cref="DropCollectionAsync(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace</param>
    public Task DropCollectionAsync(string collectionName, DatabaseCommandOptions options)
    {
        return DropCollectionAsync(collectionName, options, false);
    }

    private async Task DropCollectionAsync(string collectionName, DatabaseCommandOptions options, bool runSynchronously)
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

    public Task<Table<TRow>> CreateTableAsync<TRow>() where TRow : class, new()
    {
        return CreateTableAsync<TRow>(null as DatabaseCommandOptions);
    }

    public Task<Table<TRow>> CreateTableAsync<TRow>(DatabaseCommandOptions options) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return CreateTableAsync<TRow>(tableName, options);
    }

    public Task<Table<TRow>> CreateTableAsync<TRow>(string tableName) where TRow : class, new()
    {
        return CreateTableAsync<TRow>(tableName, null);
    }

    public Task<Table<TRow>> CreateTableAsync<TRow>(string tableName, DatabaseCommandOptions options) where TRow : class, new()
    {
        var definition = TableDefinition.CreateTableDefinition<TRow>();
        return CreateTableAsync<TRow>(tableName, definition, options, false);
    }

    public Task<Table<Row>> CreateTableAsync(string tableName, TableDefinition definition)
    {
        return CreateTableAsync(tableName, definition, null);
    }

    public Task<Table<Row>> CreateTableAsync(string tableName, TableDefinition definition, DatabaseCommandOptions options)
    {
        if (definition.PrimaryKey == null)
        {
            throw new InvalidOperationException("No primary key defined for table class. Please use definition.AddPrimaryKey() or definition.AddCompoundPrimaryKey()");
        }
        return CreateTableAsync<Row>(tableName, definition, options, false);
    }

    private async Task<Table<TRow>> CreateTableAsync<TRow>(string tableName, TableDefinition definition, DatabaseCommandOptions options, bool runSynchronously) where TRow : class
    {

        var payload = new TableCommandPayload
        {
            Name = tableName,
            Definition = definition
        };
        var command = CreateCommand("createTable")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(options);
        await command.RunAsyncReturnDictionary(runSynchronously).ConfigureAwait(false);
        return GetTable<TRow>(tableName, options);
    }

    /// <summary>
    /// Returns a reference to the table with the name defined by a [TableName] attribute on the type, or the type name if no attribute is present.
    /// </summary>
    /// <returns>A table class allowing performing operations on the table.</returns>
    public Table<TRow> GetTable<TRow>() where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return GetTable<TRow>(tableName);
    }

    /// <inheritdoc cref="GetTable{TRow}()" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Table<TRow> GetTable<TRow>(DatabaseCommandOptions options) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return GetTable<TRow>(tableName, options);
    }

    /// <summary>
    /// Returns a reference to the table with the specified name.
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public Table<Row> GetTable(string tableName)
    {
        return GetTable(tableName, null);
    }

    /// <inheritdoc cref="GetTable(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Table<Row> GetTable(string tableName, DatabaseCommandOptions options)
    {
        return GetTable<Row>(tableName, options);
    }

    /// <inheritdoc cref="GetTable(string)" />
    /// <typeparam name="T">The type of the document stored in the referenced table</typeparam>
    public Table<T> GetTable<T>(string tableName) where T : class
    {
        return GetTable<T>(tableName, null);
    }

    /// <inheritdoc cref="GetTable{T}(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Table<TRow> GetTable<TRow>(string tableName, DatabaseCommandOptions options) where TRow : class
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        return new Table<TRow>(tableName, this, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync{TRow}()"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync{TRow}()"/>
    public void DropTable<TRow>() where TRow : class, new()
    {
        DropTable<TRow>(false, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync{TRow}(DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync{TRow}(DatabaseCommandOptions)"/>
    public void DropTable<TRow>(DatabaseCommandOptions options) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        DropTable(tableName, false, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync(string)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync(string)"/>
    public void DropTable(string tableName)
    {
        DropTable(tableName, false, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync(string, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync(string, DatabaseCommandOptions)"/>
    public void DropTable(string tableName, DatabaseCommandOptions options)
    {
        DropTable(tableName, false, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync{TRow}(bool)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync{TRow}(bool)"/>
    public void DropTable<TRow>(bool onlyIfExists) where TRow : class, new()
    {
        DropTable<TRow>(onlyIfExists, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync{TRow}(bool, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync{TRow}(bool, DatabaseCommandOptions)"/>
    public void DropTable<TRow>(bool onlyIfExists, DatabaseCommandOptions options) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        DropTable(tableName, onlyIfExists, options);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync(string, bool)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync(string, bool)"/>
    public void DropTable(string tableName, bool onlyIfExists)
    {
        DropTable(tableName, onlyIfExists, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableAsync(string, bool, DatabaseCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableAsync(string, bool, DatabaseCommandOptions)"/>
    public void DropTable(string tableName, bool onlyIfExists, DatabaseCommandOptions options)
    {
        DropTableAsync(tableName, onlyIfExists, options, true).ResultSync();
    }




    /// <summary>
    /// Drops the table with the name defined by a [TableName] attribute on the TRowtype, or the type name if no attribute is present.
    /// </summary>
    /// <typeparam name="TRow"></typeparam>
    public Task DropTableAsync<TRow>() where TRow : class, new()
    {
        return DropTableAsync<TRow>(false, null);
    }

    /// <inheritdoc cref="DropTableAsync{TRow}()" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Task DropTableAsync<TRow>(DatabaseCommandOptions options) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return DropTableAsync(tableName, false, options, false);
    }

    /// <summary>
    /// Drops the table with the specified name.
    /// </summary>
    /// <param name="tableName"></param>
    public Task DropTableAsync(string tableName)
    {
        return DropTableAsync(tableName, false, null, false);
    }

    /// <inheritdoc cref="DropTableAsync(string)" />
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Task DropTableAsync(string tableName, DatabaseCommandOptions options)
    {
        return DropTableAsync(tableName, false, options, false);
    }

    /// <summary>
    /// Drops the table with the name defined by a [TableName] attribute on the TRowtype, or the type name if no attribute is present.
    /// </summary>
    /// <param name="onlyIfExists">If true, the command will not error if the table does not exist.</param>
    /// <typeparam name="TRow"></typeparam>
    public Task DropTableAsync<TRow>(bool onlyIfExists) where TRow : class, new()
    {
        return DropTableAsync<TRow>(onlyIfExists, null);
    }

    /// <inheritdoc cref="DropTableAsync{TRow}()" />
    /// <param name="onlyIfExists">If true, the command will not error if the table does not exist.</param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Task DropTableAsync<TRow>(bool onlyIfExists, DatabaseCommandOptions options) where TRow : class, new()
    {
        var tableName = TableDefinition.GetTableName<TRow>();
        return DropTableAsync(tableName, onlyIfExists, options, false);
    }

    /// <summary>
    /// Drops the table with the specified name.
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="onlyIfExists">If true, the command will not error if the table does not exist.</param>
    public Task DropTableAsync(string tableName, bool onlyIfExists)
    {
        return DropTableAsync(tableName, onlyIfExists, null, false);
    }

    /// <inheritdoc cref="DropTableAsync(string)" />
    /// <param name="tableName"></param>
    /// <param name="onlyIfExists">If true, the command will not error if the table does not exist.</param>
    /// <param name="options">The options to use for the command, useful for overriding the keyspace, for example.</param>
    public Task DropTableAsync(string tableName, bool onlyIfExists, DatabaseCommandOptions options)
    {
        return DropTableAsync(tableName, onlyIfExists, options, false);
    }

    private async Task DropTableAsync(string tableName, bool onlyIfExists, DatabaseCommandOptions options, bool runSynchronously)
    {
        var payload = new
        {
            name = tableName,
            options = new
            {
                ifExists = onlyIfExists
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
        return ListTableNamesAsync(options, true, false);
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
    /// Synchronous version of <see cref="DropTableIndexAsync(string)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableIndexAsync(string)"/>
    public void DropTableIndex(string indexName)
    {
        DropTableIndex(indexName, null);
    }

    /// <summary>
    /// Synchronous version of <see cref="DropTableIndexAsync(string, DropIndexCommandOptions)"/>
    /// </summary>
    /// <inheritdoc cref="DropTableIndexAsync(string, DropIndexCommandOptions)"/>
    public void DropTableIndex(string indexName, DropIndexCommandOptions commandOptions)
    {
        DropTableIndexAsync(indexName, commandOptions, true).ResultSync();
    }

    /// <summary>
    /// Drops an index on the table.
    /// </summary>
    /// <param name="indexName">The name of the index to drop</param>
    public Task DropTableIndexAsync(string indexName)
    {
        return DropTableIndexAsync(indexName, null, false);
    }

    /// <inheritdoc cref="DropTableIndexAsync(string)"/>
    /// <param name="commandOptions"></param>
    public Task DropTableIndexAsync(string indexName, DropIndexCommandOptions commandOptions)
    {
        return DropTableIndexAsync(indexName, commandOptions, false);
    }

    private async Task DropTableIndexAsync(string indexName, DropIndexCommandOptions commandOptions, bool runSynchronously)
    {
        var payload = new
        {
            name = indexName,
            options = new
            {
                ifExists = commandOptions?.SkipIfNotExists ?? false,
            }
        };
        var command = CreateCommand("dropIndex")
            .WithPayload(payload)
            .WithTimeoutManager(new TableAdminTimeoutManager())
            .AddCommandOptions(commandOptions);
        await command.RunAsyncReturnStatus<Dictionary<string, int>>(runSynchronously).ConfigureAwait(false);
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
