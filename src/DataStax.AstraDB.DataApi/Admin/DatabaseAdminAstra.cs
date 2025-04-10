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
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Admin
{
    /// <summary>
    /// Provides metadata about an embedding provider registered in Astra,
    /// including its name, description, version, and supported model identifiers.
    /// </summary>
    /// <example>
    /// <code>
    /// var provider = new EmbeddingProviderInfo { Name = "OpenAI", Description = "OpenAI API", Version = "1.0.0", SupportedModels = new List&lt;string&gt; { "text-embedding-ada-002" } };
    /// </code>
    /// </example>
    public class EmbeddingProviderInfo
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string Version { get; set; }
        public List<string> SupportedModels { get; set; }
    }

    /// <summary>
    /// Provides administrative operations for an Astra database, including keyspace management
    /// and embedding provider discovery. This class is used internally by the Astra client library
    /// to execute privileged actions against a specific database.
    /// </summary>
    /// <remarks>
    /// This implementation of <see cref="IDatabaseAdmin"/> supports synchronous and asynchronous
    /// methods for listing, creating, and dropping keyspaces, as well as querying embedding providers.
    /// </remarks>
    public class DatabaseAdminAstra : IDatabaseAdmin
    {
        private readonly Guid _id;
        private readonly Database _database;
        private readonly CommandOptions _adminOptions;
        private readonly DataApiClient _client;
        private CommandOptions[] _optionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };

        internal DatabaseAdminAstra(Guid id, DataApiClient client, CommandOptions adminOptions)
        {
            Guard.NotNull(client, nameof(client));
            _client = client;
            _adminOptions = adminOptions;
            _database = _client.GetDatabase(id);
            _id = id;
        }

        internal DatabaseAdminAstra(Database database, DataApiClient client, CommandOptions adminOptions)
        {
            Guard.NotNull(client, nameof(client));
            _client = client;
            _adminOptions = adminOptions;
            _database = database;
            _id = _database.DatabaseId;
        }

        /// <summary>
        /// Gets the <see cref="Database"/> instance associated with this admin context.
        /// </summary>
        /// <returns>
        /// The connected <see cref="Database"/> instance.
        /// </returns>
        /// <example>
        /// <code>
        /// var database = admin.GetDatabase();
        /// </code>
        /// </example>
        public Database GetDatabase()
        {
            return _database;
        }

        /// <summary>
        /// Gets the API endpoint URL for the associated database.
        /// </summary>
        /// <returns>The API endpoint as a string.</returns>
        /// <example>
        /// <code>
        /// string endpoint = admin.GetApiEndpoint();
        /// </code>
        /// </example>
        public string GetApiEndpoint()
        {
            return _database.ApiEndpoint;
        }

        /// <summary>
        /// Lists the names of all keyspaces in the database.
        /// </summary>
        /// <returns>A collection of keyspace names.</returns>
        /// <example>
        /// <code>
        /// IEnumerable&lt;string&gt; keyspaces = admin.ListKeyspaceNames();
        /// </code>
        /// </example>
        public IEnumerable<string> ListKeyspaceNames()
        {
            return ListKeyspaceNamesAsync(true, null).ResultSync();
        }

        /// <summary>
        /// Asynchronously lists the names of all keyspaces in the database.
        /// </summary>
        /// <returns>A task that resolves to a collection of keyspace names.</returns>
        /// <example>
        /// <code>
        /// var keyspaces = await admin.ListKeyspaceNamesAsync();
        /// </code>
        /// </example>
        public Task<IEnumerable<string>> ListKeyspaceNamesAsync()
        {
            return ListKeyspaceNamesAsync(false, null);
        }

        /// <summary>
        /// Lists the names of all keyspaces using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A collection of keyspace names.</returns>
        /// <example>
        /// <code>
        /// var keyspaces = admin.ListKeyspaceNames(options);
        /// </code>
        /// </example>
        public IEnumerable<string> ListKeyspaceNames(CommandOptions options)
        {
            return ListKeyspaceNamesAsync(true, options).ResultSync();
        }

        /// <summary>
        /// Asynchronously lists the names of all keyspaces using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A task that resolves to a collection of keyspace names.</returns>
        /// <example>
        /// <code>
        /// var keyspaces = await admin.ListKeyspaceNamesAsync(options);
        /// </code>
        /// </example>
        public Task<IEnumerable<string>> ListKeyspaceNamesAsync(CommandOptions options)
        {
            return ListKeyspaceNamesAsync(false, options);
        }

        internal async Task<IEnumerable<string>> ListKeyspaceNamesAsync(bool runSynchronously, CommandOptions options)
        {
            var databaseInfo = await _client.GetAstraDatabasesAdmin().GetDatabaseInfoAsync(_id, options, runSynchronously);
            return databaseInfo.Info.Keyspaces;
        }

        /// <summary>
        /// Creates a new keyspace with the specified name.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <example>
        /// <code>
        /// admin.CreateKeyspace("myKeyspace");
        /// </code>
        /// </example>
        public void CreateKeyspace(string keyspace)
        {
            CreateKeyspaceAsync(keyspace, false, null, true).ResultSync();
        }

        /// <summary>
        /// Creates a new keyspace with the specified name and optionally updates the database's keyspace reference.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace in the command options.</param>
        /// <example>
        /// <code>
        /// admin.CreateKeyspace("myKeyspace", true);
        /// </code>
        /// </example>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace)
        {
            CreateKeyspaceAsync(keyspace, updateDBKeyspace, null, true).ResultSync();
        }

        /// <summary>
        /// Creates a new keyspace with the specified name using the provided command options.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// admin.CreateKeyspace("myKeyspace", options);
        /// </code>
        /// </example>
        public void CreateKeyspace(string keyspace, CommandOptions options)
        {
            CreateKeyspaceAsync(keyspace, false, options, true).ResultSync();
        }

        /// <summary>
        /// Creates a new keyspace with the specified name, optionally updating the database's keyspace reference,
        /// and applying the given command options.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace in the command options.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// admin.CreateKeyspace("myKeyspace", true, options);
        /// </code>
        /// </example>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, CommandOptions options)
        {
            CreateKeyspaceAsync(keyspace, updateDBKeyspace, options, true).ResultSync();
        }

        /// <summary>
        /// Asynchronously creates a new keyspace with the specified name.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace");
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace)
        {
            return CreateKeyspaceAsync(keyspace, false, null, false);
        }

        /// <summary>
        /// Asynchronously creates a new keyspace with the specified name and optionally updates the database's keyspace reference.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace in the command options.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", true);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, null, false);
        }

        /// <summary>
        /// Asynchronously creates a new keyspace with the specified name using the provided command options.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", options);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, false, options, false);
        }

        /// <summary>
        /// Asynchronously creates a new keyspace with the specified name, optionally updating the database's keyspace reference,
        /// and applying the given command options.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace in the command options.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", true, options);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, options, false);
        }

        internal async Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, CommandOptions options, bool runSynchronously)
        {
            options ??= new CommandOptions();
            options.IncludeKeyspaceInUrl = false;
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            bool exists = await KeyspaceExistsAsync(keyspace, options, runSynchronously).ConfigureAwait(false);
            if (exists)
            {
                throw new InvalidOperationException($"Keyspace {keyspace} already exists");
            }

            var command = CreateCommandAdmin()
                .AddUrlPath("databases")
                .AddUrlPath(_id.ToString())
                .AddUrlPath("keyspaces")
                .AddUrlPath(keyspace)
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);

            if (updateDBKeyspace && options != null)
            {
                options.Keyspace = keyspace;
            }
        }

        /// <summary>
        /// Drops the keyspace with the specified name.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <example>
        /// <code>
        /// admin.DropKeyspace("myKeyspace");
        /// </code>
        /// </example>
        public void DropKeyspace(string keyspace)
        {
            DropKeyspaceAsync(keyspace, null, true).ResultSync();
        }

        /// <summary>
        /// Asynchronously drops the keyspace with the specified name.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <example>
        /// <code>
        /// await admin.DropKeyspaceAsync("myKeyspace");
        /// </code>
        /// </example>
        public Task DropKeyspaceAsync(string keyspace)
        {
            return DropKeyspaceAsync(keyspace, null, false);
        }

        /// <summary>
        /// Drops the keyspace with the specified name using the provided command options.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// admin.DropKeyspace("myKeyspace", options);
        /// </code>
        /// </example>
        public void DropKeyspace(string keyspace, CommandOptions options)
        {
            DropKeyspaceAsync(keyspace, options, true).ResultSync();
        }

        /// <summary>
        /// Asynchronously drops the keyspace with the specified name using the provided command options.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <example>
        /// <code>
        /// await admin.DropKeyspaceAsync("myKeyspace", options);
        /// </code>
        /// </example>
        public Task DropKeyspaceAsync(string keyspace, CommandOptions options)
        {
            return DropKeyspaceAsync(keyspace, options, false);
        }

        internal async Task DropKeyspaceAsync(string keyspace, CommandOptions options, bool runSynchronously)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            var command = CreateCommandAdmin()
                .AddUrlPath($"databases/{_id}/keyspaces/{keyspace}")
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(HttpMethod.Delete, runSynchronously)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Checks whether a keyspace with the specified name exists.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to check.</param>
        /// <returns><c>true</c> if the keyspace exists; otherwise, <c>false</c>.</returns>
        /// <example>
        /// <code>
        /// bool exists = admin.KeyspaceExists("myKeyspace");
        /// </code>
        /// </example>
        public bool KeyspaceExists(string keyspace)
        {
            return KeyspaceExistsAsync(keyspace, null, true).ResultSync();
        }

        /// <summary>
        /// Asynchronously checks whether a keyspace with the specified name exists.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to check.</param>
        /// <returns>A task that resolves to <c>true</c> if the keyspace exists; otherwise, <c>false</c>.</returns>
        /// <example>
        /// <code>
        /// bool exists = await admin.KeyspaceExistsAsync("myKeyspace");
        /// </code>
        /// </example>
        public Task<bool> KeyspaceExistsAsync(string keyspace)
        {
            return KeyspaceExistsAsync(keyspace, null, false);
        }

        internal async Task<bool> KeyspaceExistsAsync(string keyspace, CommandOptions options, bool runSynchronously)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));
            var keyspaces = await ListKeyspaceNamesAsync(runSynchronously, options).ConfigureAwait(false);
            return keyspaces.Contains(keyspace);
        }

        /// <summary>
        /// Finds and returns available embedding providers for the current database.
        /// </summary>
        /// <returns>A <see cref="FindEmbeddingProvidersResult"/> containing the discovered providers.</returns>
        /// <example>
        /// <code>
        /// var providers = admin.FindEmbeddingProviders();
        /// </code>
        /// </example>
        public FindEmbeddingProvidersResult FindEmbeddingProviders()
        {
            return FindEmbeddingProvidersAsync(null, true).ResultSync();
        }

        /// <summary>
        /// Asynchronously finds and returns available embedding providers for the current database.
        /// </summary>
        /// <returns>
        /// A task that resolves to a <see cref="FindEmbeddingProvidersResult"/> containing the discovered providers.
        /// </returns>
        /// <example>
        /// <code>
        /// var providers = await admin.FindEmbeddingProvidersAsync();
        /// </code>
        /// </example>
        public Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync()
        {
            return FindEmbeddingProvidersAsync(null, false);
        }

        /// <summary>
        /// Finds and returns available embedding providers for the current database using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A <see cref="FindEmbeddingProvidersResult"/> containing the discovered providers.</returns>
        /// <example>
        /// <code>
        /// var providers = admin.FindEmbeddingProviders(options);
        /// </code>
        /// </example>
        public FindEmbeddingProvidersResult FindEmbeddingProviders(CommandOptions options)
        {
            return FindEmbeddingProvidersAsync(options, true).ResultSync();
        }

        /// <summary>
        /// Asynchronously finds and returns available embedding providers for the current database
        /// using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>
        /// A task that resolves to a <see cref="FindEmbeddingProvidersResult"/> containing the discovered providers.
        /// </returns>
        /// <example>
        /// <code>
        /// var providers = await admin.FindEmbeddingProvidersAsync(options);
        /// </code>
        /// </example>
        public Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(CommandOptions options)
        {
            return FindEmbeddingProvidersAsync(options, false);
        }

        /// <summary>
        /// Represents the raw response returned from the embedding provider discovery request.
        /// </summary>
        public class FindEmbeddingProvidersResponse
        {
            public Status status { get; set; }
        }

        /// <summary>
        /// Contains the status payload of the embedding provider discovery response,
        /// including a dictionary of discovered embedding providers.
        /// </summary>
        public class Status
        {
            public Dictionary<string, EmbeddingProvider> embeddingProviders { get; set; }
        }
        internal async Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(CommandOptions options, bool runSynchronously)
        {
            var command = CreateCommandEmbedding()
               .AddCommandOptions(options)
               .WithPayload(new { findEmbeddingProviders = new { } });

            var response = await command
                .RunAsyncRaw<FindEmbeddingProvidersResponse>(HttpMethod.Post, runSynchronously)
                .ConfigureAwait(false);

            var result = new FindEmbeddingProvidersResult();

            if (response?.status?.embeddingProviders is Dictionary<string, EmbeddingProvider> providers)
            {
                foreach (var kvp in providers)
                {
                    result.EmbeddingProviders[kvp.Key] = kvp.Value;
                }
            }

            return result;
        }

        private Command CreateCommandDb()
        {
            return new Command(_database.Client, _optionsTree, new DatabaseCommandUrlBuilder(_database, null));
        }

        private Command CreateCommandAdmin()
        {
            return new Command(_database.Client, _optionsTree, new AdminCommandUrlBuilder());
        }

        private Command CreateCommandEmbedding()
        {
            return new Command(_database.Client, _optionsTree, new EmbeddingCommandUrlBuilder(_database));
        }
    }
}

