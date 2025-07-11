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
        private readonly DataApiClient _client;
        private CommandOptions[] _optionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };

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
            var databaseInfo = await _client.GetAstraDatabasesAdmin().GetDatabaseInfoAsync(_id, options, runSynchronously);
            return databaseInfo.Keyspaces;
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync()"/>
        public void CreateKeyspace(string keyspace)
        {
            CreateKeyspace(keyspace, false);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace)
        {
            CreateKeyspace(keyspace, updateDBKeyspace, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, CommandOptions)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, CommandOptions)"/>
        public void CreateKeyspace(string keyspace, CommandOptions options)
        {
            CreateKeyspace(keyspace, false, true, options);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool, bool)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool, bool)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion)
        {
            CreateKeyspace(keyspace, updateDBKeyspace, waitForCompletion, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool, CommandOptions)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool, CommandOptions)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, CommandOptions options)
        {
            CreateKeyspace(keyspace, updateDBKeyspace, true, options);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool, bool, CommandOptions)"/>
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool, bool, CommandOptions)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options)
        {
            CreateKeyspaceAsync(keyspace, updateDBKeyspace, waitForCompletion, options).ResultSync();
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
        public Task CreateKeyspaceAsync(string keyspace)
        {
            return CreateKeyspaceAsync(keyspace, false, true, null);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync()"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace in the command options.</param>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", true);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, false, null);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync()"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace in the command options.</param>
        /// <param name="waitForCompletion">Whether or not to wait for the keyspace to be created before returning.</param>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", true);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, waitForCompletion, null);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync()"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", options);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, false, false, options, false);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync()"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="waitForCompletion">Whether or not to wait for the keyspace to be created before returning.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", true, options);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, bool waitForCompletion, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, false, waitForCompletion, options, false);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync()"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace in the command options.</param>
        /// <param name="waitForCompletion">Whether or not to wait for the keyspace to be created before returning.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// await admin.CreateKeyspaceAsync("myKeyspace", true, true, options);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, waitForCompletion, options, false);
        }

        internal async Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options, bool runSynchronously)
        {
            options ??= new CommandOptions();
            options.IncludeKeyspaceInUrl = false;
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            bool exists = await DoesKeyspaceExistAsync(keyspace, options, runSynchronously).ConfigureAwait(false);
            if (exists)
            {
                throw new InvalidOperationException($"Keyspace {keyspace} already exists");
            }

            var command = CreateCommandAdmin()
                .AddUrlPath("databases")
                .AddUrlPath(_id.ToString())
                .AddUrlPath("keyspaces")
                .AddUrlPath(keyspace)
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);

            if (updateDBKeyspace && options != null)
            {
                options.Keyspace = keyspace;
            }

            if (waitForCompletion)
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
        /// <example>
        /// <code>
        /// admin.DropKeyspace("myKeyspace");
        /// </code>
        /// </example>
        public void DropKeyspace(string keyspace)
        {
            DropKeyspace(keyspace, true, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, bool)"/>
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string, bool)"/>
        /// <example>
        /// <code>
        /// admin.DropKeyspace("myKeyspace", false);
        /// </code>
        /// </example>
        public void DropKeyspace(string keyspace, bool waitForCompletion)
        {
            DropKeyspace(keyspace, waitForCompletion, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, CommandOptions)"/>
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string, CommandOptions)"/>
        /// <example>
        /// <code>
        /// admin.DropKeyspace("myKeyspace", options);
        /// </code>
        /// </example>
        public void DropKeyspace(string keyspace, CommandOptions options)
        {
            DropKeyspace(keyspace, true, options);
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, bool, CommandOptions)"/>
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string, bool, CommandOptions)"/>
        public void DropKeyspace(string keyspace, bool waitForCompletion, CommandOptions options)
        {
            DropKeyspaceAsync(keyspace, waitForCompletion, options, true).ResultSync();
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
        /// This method will wait for the keyspace to be dropped before returning.
        /// If you do not want to wait for the keyspace to be dropped, use the <see cref="DropKeyspaceAsync(string, bool, CommandOptions)"/> method.
        /// </remarks>
        public Task DropKeyspaceAsync(string keyspace)
        {
            return DropKeyspaceAsync(keyspace, true, null);
        }

        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        /// <param name="waitForCompletion">Whether or not to wait for the keyspace to be dropped before returning.</param>
        /// <example>
        /// <code>
        /// await admin.DropKeyspaceAsync("myKeyspace", false);
        /// </code>
        /// </example>
        public Task DropKeyspaceAsync(string keyspace, bool waitForCompletion)
        {
            return DropKeyspaceAsync(keyspace, waitForCompletion, null);
        }

        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        /// <param name="options">Optional settings that influence request execution.</param>

        /// <example>
        /// <code>
        /// await admin.DropKeyspaceAsync("myKeyspace", options);
        /// </code>
        /// </example>
        /// <remarks>
        /// This method will wait for the keyspace to be dropped before returning.
        /// If you do not want to wait for the keyspace to be dropped, use the <see cref="DropKeyspaceAsync(string, bool, CommandOptions)"/> method.
        /// </remarks>
        public Task DropKeyspaceAsync(string keyspace, CommandOptions options)
        {
            return DropKeyspaceAsync(keyspace, true, options, false);
        }

        /// <inheritdoc cref="DropKeyspaceAsync(string, CommandOptions)"/>
        /// <param name="waitForCompletion">Whether or not to wait for the keyspace to be dropped before returning.</param>
        /// <example>
        /// <code>
        /// await admin.DropKeyspaceAsync("myKeyspace", true, options);
        /// </code>
        /// </example>
        public Task DropKeyspaceAsync(string keyspace, bool waitForCompletion, CommandOptions options)
        {
            return DropKeyspaceAsync(keyspace, waitForCompletion, options, false);
        }

        internal async Task DropKeyspaceAsync(string keyspace, bool waitForCompletion, CommandOptions options, bool runSynchronously)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            var command = CreateCommandAdmin()
                .AddUrlPath($"databases/{_id}/keyspaces/{keyspace}")
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .AddCommandOptions(options);

            await command.RunAsyncRaw<Command.EmptyResult>(HttpMethod.Delete, runSynchronously)
                .ConfigureAwait(false);

            if (waitForCompletion)
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

        internal async Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(CommandOptions options, bool runSynchronously)
        {
            var command = CreateCommandEmbedding()
               .AddCommandOptions(options)
               .WithTimeoutManager(new DatabaseAdminTimeoutManager())
               .WithPayload(new { findEmbeddingProviders = new { } });

            var response = await command
                .RunAsyncReturnStatus<FindEmbeddingProvidersResult>(runSynchronously)
                .ConfigureAwait(false);

            return response.Result;
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