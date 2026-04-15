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
        private readonly DataAPIClient _client;
        private CommandOptions[] _optionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };
        private static readonly CommandOptions _devOpsApiOptions = new CommandOptions { SerializeDateAsDollarDate = false };

        internal DatabaseAdminAstra(Database database, DataAPIClient client, CommandOptions adminOptions)
        {
            Guard.NotNull(client, nameof(client));
            if (database.DatabaseId == null)
            {
                throw new ArgumentException("Database must have a valid DatabaseId to use DatabaseAdminAstra", nameof(database));
            }
            _client = client;
            _adminOptions = adminOptions;
            _database = database;
            _id = _database.DatabaseId.Value;
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
        /// IEnumerable&lt;string&gt; keyspaces = admin.ListKeyspaces();
        /// </code>
        /// </example>
        public IEnumerable<string> ListKeyspaces()
        {
            return ListKeyspacesAsync(true, null).ResultSync();
        }

        /// <summary>
        /// Asynchronously lists the names of all keyspaces in the database.
        /// </summary>
        /// <returns>A task that resolves to a collection of keyspace names.</returns>
        /// <example>
        /// <code>
        /// var keyspaces = await admin.ListKeyspacesAsync();
        /// </code>
        /// </example>
        public Task<IEnumerable<string>> ListKeyspacesAsync()
        {
            return ListKeyspacesAsync(false, null);
        }

        /// <summary>
        /// Lists the names of all keyspaces using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A collection of keyspace names.</returns>
        /// <example>
        /// <code>
        /// var keyspaces = admin.ListKeyspaces(options);
        /// </code>
        /// </example>
        public IEnumerable<string> ListKeyspaces(CommandOptions options)
        {
            return ListKeyspacesAsync(true, options).ResultSync();
        }

        /// <summary>
        /// Asynchronously lists the names of all keyspaces using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A task that resolves to a collection of keyspace names.</returns>
        /// <example>
        /// <code>
        /// var keyspaces = await admin.ListKeyspacesAsync(options);
        /// </code>
        /// </example>
        public Task<IEnumerable<string>> ListKeyspacesAsync(CommandOptions options)
        {
            return ListKeyspacesAsync(false, options);
        }

        internal async Task<IEnumerable<string>> ListKeyspacesAsync(bool runSynchronously, CommandOptions options)
        {
            var databaseInfo = await _client.GetAstraDatabasesAdmin(_adminOptions).GetDatabaseInfoAsync(_id.ToString(), options, runSynchronously);
            return databaseInfo.Keyspaces;
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <remarks>
        /// This method, by default, will wait for the operation to complete on the server side.
        /// Use the options' waitForCompletion attribute to control this behaviour.
        /// </remarks>
        public void CreateKeyspace(string keyspace)
        {
            CreateKeyspace(keyspace, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, CreateKeyspaceCommandOptions)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, CreateKeyspaceCommandOptions)"/>
        public void CreateKeyspace(string keyspace, CreateKeyspaceCommandOptions options)
        {
            CreateKeyspaceAsync(keyspace, options, true).ResultSync();
        }

        /// <summary>
        /// Creates a new keyspace with the specified name.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace");
        /// </code>
        /// </example>
        /// <remarks>
        /// This method, by default, will wait for the operation to complete on the server side.
        /// Use the options' waitForCompletion attribute to control this behaviour.
        /// </remarks>
        public Task CreateKeyspaceAsync(string keyspace)
        {
            return CreateKeyspaceAsync(keyspace, null);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", options);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceCommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, options, false);
        }

        internal async Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceCommandOptions options, bool runSynchronously)
        {
            options ??= new CreateKeyspaceCommandOptions();
            options.IncludeKeyspaceInUrl = false;
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            var command = CreateCommandAdmin()
                .AddUrlPath("databases")
                .AddUrlPath(_id.ToString())
                .AddUrlPath("keyspaces")
                .AddUrlPath(keyspace)
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);

            if (options.updateDBKeyspace)
            {
                _database.UseKeyspace(keyspace);
            }

            if (options.waitForCompletion)
            {
                try
                {
                    await Wait.WaitForProcess(() => DoesKeyspaceExistAsync(keyspace, options, runSynchronously)).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to create keyspace {keyspace} within the allotted time", e);
                }
            }
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string)"/>
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        /// <remarks>
        /// This method, by default, will wait for the operation to complete on the server side.
        /// Use the options' waitForCompletion attribute to control this behaviour.
        /// </remarks>
        public void DropKeyspace(string keyspace)
        {
            DropKeyspace(keyspace, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, BlockingCommandOptions)"/>
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string, BlockingCommandOptions)"/>
        public void DropKeyspace(string keyspace, BlockingCommandOptions options)
        {
            DropKeyspaceAsync(keyspace, options, true).ResultSync();
        }

        /// <summary>
        /// Drops the keyspace with the specified name.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <example>
        /// <code>
        /// await admin.DropKeyspaceAsync("myKeyspace");
        /// </code>
        /// </example>
        /// <remarks>
        /// This method, by default, will wait for the operation to complete on the server side.
        /// Use the options' waitForCompletion attribute to control this behaviour.
        /// </remarks>
        public Task DropKeyspaceAsync(string keyspace)
        {
            return DropKeyspaceAsync(keyspace, null);
        }

        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// await admin.DropKeyspaceAsync("myKeyspace", options);
        /// </code>
        /// </example>
        public Task DropKeyspaceAsync(string keyspace, BlockingCommandOptions options)
        {
            return DropKeyspaceAsync(keyspace, options, false);
        }

        internal async Task DropKeyspaceAsync(string keyspace, BlockingCommandOptions options, bool runSynchronously)
        {
            options ??= new BlockingCommandOptions();
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            var command = CreateCommandAdmin()
                .AddUrlPath($"databases/{_id}/keyspaces/{keyspace}")
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(HttpMethod.Delete, runSynchronously)
                .ConfigureAwait(false);

            if (options.waitForCompletion)
            {
                try
                {
                    await Wait.WaitForProcess(async () => !await DoesKeyspaceExistAsync(keyspace, options, runSynchronously).ConfigureAwait(false)).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to drop keyspace {keyspace} within the allotted time", e);
                }
            }
        }

        /// <summary>
        /// Checks whether a keyspace with the specified name exists.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to check.</param>
        /// <returns><c>true</c> if the keyspace exists; otherwise, <c>false</c>.</returns>
        /// <example>
        /// <code>
        /// bool exists = admin.DoesKeyspaceExist("myKeyspace");
        /// </code>
        /// </example>
        public bool DoesKeyspaceExist(string keyspace)
        {
            return DoesKeyspaceExistAsync(keyspace, null, true).ResultSync();
        }

        /// <summary>
        /// Asynchronously checks whether a keyspace with the specified name exists.
        /// </summary>
        /// <param name="keyspace">The name of the keyspace to check.</param>
        /// <returns>A task that resolves to <c>true</c> if the keyspace exists; otherwise, <c>false</c>.</returns>
        /// <example>
        /// <code>
        /// bool exists = await admin.DoesKeyspaceExistAsync("myKeyspace");
        /// </code>
        /// </example>
        public Task<bool> DoesKeyspaceExistAsync(string keyspace)
        {
            return DoesKeyspaceExistAsync(keyspace, null, false);
        }

        internal async Task<bool> DoesKeyspaceExistAsync(string keyspace, CommandOptions options, bool runSynchronously)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));
            var keyspaces = await ListKeyspacesAsync(runSynchronously, options).ConfigureAwait(false);
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
        public FindEmbeddingProvidersResult FindEmbeddingProviders(FindEmbeddingProvidersCommandOptions options)
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
        public Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(FindEmbeddingProvidersCommandOptions options)
        {
            return FindEmbeddingProvidersAsync(options, false);
        }

        internal async Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(FindEmbeddingProvidersCommandOptions options, bool runSynchronously)
        {

            if (options == null){
                options = new FindEmbeddingProvidersCommandOptions {};
            }

            object epPayload = options.FilterModelStatus == null
                ? new { }
                : new
                {
                    options = new
                    {
                        filterModelStatus = options.FilterModelStatus.Value.ToApiString()
                    }
                };

            var command = CreateCommandEmbedding()
                .AddCommandOptions(options)
                .WithTimeoutManager(new DatabaseAdminTimeoutManager())
                .WithPayload(new { findEmbeddingProviders = epPayload });

            var response = await command
                .RunAsyncReturnStatus<FindEmbeddingProvidersResult>(runSynchronously)
                .ConfigureAwait(false);

            return response.Result;
        }
        
        /// <summary>
        /// Finds and returns available reranking providers for the current database.
        /// </summary>
        /// <returns>A <see cref="FindRerankingProvidersResult"/> containing the discovered providers.</returns>
        /// <example>
        /// <code>
        /// var providers = admin.FindRerankingProviders();
        /// </code>
        /// </example>
        public FindRerankingProvidersResult FindRerankingProviders()
        {
            return FindRerankingProvidersAsync(null, true).ResultSync();
        }
        
        /// <summary>
        /// Asynchronously finds and returns available reranking providers for the current database.
        /// </summary>
        /// <returns>
        /// A task that resolves to a <see cref="FindRerankingProvidersResult"/> containing the discovered providers.
        /// </returns>
        /// <example>
        /// <code>
        /// var providers = await admin.FindRerankingProvidersAsync();
        /// </code>
        /// </example>
        public Task<FindRerankingProvidersResult> FindRerankingProvidersAsync()
        {
            return FindRerankingProvidersAsync(null, false);
        }
        
        /// <summary>
        /// Finds and returns available reranking providers for the current database using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A <see cref="FindRerankingProvidersResult"/> containing the discovered providers.</returns>
        /// <example>
        /// <code>
        /// var providers = admin.FindRerankingProviders(options);
        /// </code>
        /// </example>
        public FindRerankingProvidersResult FindRerankingProviders(FindRerankingProvidersCommandOptions options)
        {
            return FindRerankingProvidersAsync(options, true).ResultSync();
        }
        
        /// <summary>
        /// Asynchronously finds and returns available reranking providers for the current database
        /// using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>
        /// A task that resolves to a <see cref="FindRerankingProvidersResult"/> containing the discovered providers.
        /// </returns>
        /// <example>
        /// <code>
        /// var providers = await admin.FindRerankingProvidersAsync(options);
        /// </code>
        /// </example>
        public Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersCommandOptions options)
        {
            return FindRerankingProvidersAsync(options, false);
        }
        
        internal async Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersCommandOptions options, bool runSynchronously)
        {

            if (options == null){
                options = new FindRerankingProvidersCommandOptions {};
            }

            object epPayload = options.FilterModelStatus == null
                ? new { }
                : new
                {
                    options = new
                    {
                        filterModelStatus = options.FilterModelStatus.Value.ToApiString()
                    }
                };

            var command = CreateCommandEmbedding()
                .AddCommandOptions(options)
                .WithTimeoutManager(new DatabaseAdminTimeoutManager())
                .WithPayload(new { findRerankingProviders = epPayload });
        
            var response = await command
                .RunAsyncReturnStatus<FindRerankingProvidersResult>(runSynchronously)
                .ConfigureAwait(false);
        
            return response.Result;
        }

        private Command CreateCommandAdmin()
        {
            var options = _optionsTree.Concat(new[] { _devOpsApiOptions }).ToArray();
            return new Command(_database.Client, options, new AdminCommandUrlBuilder());
        }

        private Command CreateCommandEmbedding()
        {
            return new Command(_database.Client, _optionsTree, new EmbeddingCommandUrlBuilder(_database));
        }
    }
}
