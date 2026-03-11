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
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Admin
{
    /// <summary>
    /// Provides administrative operations for a non-Astra (DSE/HCD) database, including keyspace
    /// management and embedding/reranking provider discovery. All keyspace operations are executed
    /// via the Data API command protocol rather than the Astra DevOps API.
    /// </summary>
    /// <remarks>
    /// This implementation of <see cref="IDatabaseAdmin"/> supports synchronous and asynchronous
    /// methods for listing, creating, and dropping keyspaces, as well as querying embedding and
    /// reranking providers.
    /// </remarks>
    public class DatabaseAdminOther : IDatabaseAdmin
    {
        private readonly Guid? _id;
        private readonly Database _database;
        private readonly CommandOptions _adminOptions;
        private readonly DataApiClient _client;
        private CommandOptions[] _optionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };

        internal DatabaseAdminOther(Database database, DataApiClient client, CommandOptions adminOptions)
        {
            Guard.NotNull(client, nameof(client));
            _client = client;
            _adminOptions = adminOptions;
            _database = database;
        }

        /// <summary>
        /// Gets the <see cref="Database"/> instance associated with this admin context.
        /// </summary>
        /// <returns>The connected <see cref="Database"/> instance.</returns>
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
        /// Gets a <see cref="Database"/> instance scoped to the specified keyspace.
        /// </summary>
        /// <param name="keyspace">The keyspace to use.</param>
        /// <returns>A <see cref="Database"/> instance using the specified keyspace.</returns>
        /// <example>
        /// <code>
        /// var database = admin.GetDatabase("myKeyspace");
        /// </code>
        /// </example>
        public Database GetDatabase(string keyspace)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));
            return _client.GetDatabase(_database.ApiEndpoint, new DatabaseCommandOptions { Keyspace = keyspace });
        }

        /// <summary>
        /// Gets a <see cref="Database"/> instance scoped to the specified keyspace and authenticated with the given token.
        /// </summary>
        /// <param name="keyspace">The keyspace to use.</param>
        /// <param name="userToken">The token to use for authentication.</param>
        /// <returns>A <see cref="Database"/> instance using the specified keyspace and token.</returns>
        /// <example>
        /// <code>
        /// var database = admin.GetDatabase("myKeyspace", "myToken");
        /// </code>
        /// </example>
        public Database GetDatabase(string keyspace, string userToken)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));
            return _client.GetDatabase(_database.ApiEndpoint, new DatabaseCommandOptions { Keyspace = keyspace, Token = userToken });
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
        /// Lists the names of all keyspaces using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A collection of keyspace names.</returns>
        public IEnumerable<string> ListKeyspaces(CommandOptions options)
        {
            return ListKeyspacesAsync(true, options).ResultSync();
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
        /// Asynchronously lists the names of all keyspaces using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A task that resolves to a collection of keyspace names.</returns>
        public Task<IEnumerable<string>> ListKeyspacesAsync(CommandOptions options)
        {
            return ListKeyspacesAsync(false, options);
        }

        internal async Task<IEnumerable<string>> ListKeyspacesAsync(bool runSynchronously, CommandOptions options)
        {
            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .WithPayload(new { findKeyspaces = new { } });

            var response = await command
                .RunAsyncReturnStatus<FindKeyspacesResult>(runSynchronously)
                .ConfigureAwait(false);

            return response.Result?.Keyspaces ?? new List<string>();
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        public void CreateKeyspace(string keyspace)
        {
            CreateKeyspace(keyspace, false);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace)
        {
            CreateKeyspace(keyspace, updateDBKeyspace, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, CommandOptions)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, CommandOptions)"/>
        public void CreateKeyspace(string keyspace, CommandOptions options)
        {
            CreateKeyspace(keyspace, false, false, options);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool, bool)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool, bool)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion)
        {
            CreateKeyspace(keyspace, updateDBKeyspace, waitForCompletion, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool, CommandOptions)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool, CommandOptions)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, CommandOptions options)
        {
            CreateKeyspace(keyspace, updateDBKeyspace, false, options);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, bool, bool, CommandOptions)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, bool, bool, CommandOptions)"/>
        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options)
        {
            CreateKeyspaceAsync(keyspace, updateDBKeyspace, waitForCompletion, options, true).ResultSync();
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
            return CreateKeyspaceAsync(keyspace, false, false, null, false);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace on the database.</param>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, false, null, false);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        public Task CreateKeyspaceAsync(string keyspace, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, false, false, options, false);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace on the database.</param>
        /// <param name="waitForCompletion">Whether to wait for the keyspace to be created before returning.</param>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, waitForCompletion, null, false);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace on the database.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, false, options, false);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="updateDBKeyspace">Whether to set this keyspace as the active keyspace on the database.</param>
        /// <param name="waitForCompletion">Whether to wait for the keyspace to be created before returning.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, waitForCompletion, options, false);
        }

        internal async Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options, bool runSynchronously)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .WithPayload(new
                {
                    createKeyspace = new
                    {
                        name = keyspace,
                        options = new
                        {
                            replication = new
                            {
                                @class = "SimpleStrategy",
                                replication_factor = 1
                            }
                        }
                    }
                });

            await command
                .RunAsyncReturnStatus<object>(runSynchronously)
                .ConfigureAwait(false);

            if (updateDBKeyspace)
            {
                _database.UseKeyspace(keyspace);
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
        /// Synchronous version of <see cref="DropKeyspaceAsync(string)"/>.
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        public void DropKeyspace(string keyspace)
        {
            DropKeyspace(keyspace, false, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, bool)"/>.
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string, bool)"/>
        public void DropKeyspace(string keyspace, bool waitForCompletion)
        {
            DropKeyspace(keyspace, waitForCompletion, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, CommandOptions)"/>.
        /// </summary>
        /// <inheritdoc cref="DropKeyspaceAsync(string, CommandOptions)"/>
        public void DropKeyspace(string keyspace, CommandOptions options)
        {
            DropKeyspace(keyspace, false, options);
        }

        /// <summary>
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, bool, CommandOptions)"/>.
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
        public Task DropKeyspaceAsync(string keyspace)
        {
            return DropKeyspaceAsync(keyspace, false, null, false);
        }

        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <param name="waitForCompletion">Whether to wait for the keyspace to be dropped before returning.</param>
        public Task DropKeyspaceAsync(string keyspace, bool waitForCompletion)
        {
            return DropKeyspaceAsync(keyspace, waitForCompletion, null, false);
        }

        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        public Task DropKeyspaceAsync(string keyspace, CommandOptions options)
        {
            return DropKeyspaceAsync(keyspace, false, options, false);
        }

        /// <inheritdoc cref="DropKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to drop.</param>
        /// <param name="waitForCompletion">Whether to wait for the keyspace to be dropped before returning.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        public Task DropKeyspaceAsync(string keyspace, bool waitForCompletion, CommandOptions options)
        {
            return DropKeyspaceAsync(keyspace, waitForCompletion, options, false);
        }

        internal async Task DropKeyspaceAsync(string keyspace, bool waitForCompletion, CommandOptions options, bool runSynchronously)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .WithPayload(new
                {
                    dropKeyspace = new
                    {
                        name = keyspace
                    }
                });

            await command
                .RunAsyncReturnStatus<object>(runSynchronously)
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
            return new System.Collections.Generic.HashSet<string>(keyspaces).Contains(keyspace);
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
        /// Finds and returns available embedding providers using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A <see cref="FindEmbeddingProvidersResult"/> containing the discovered providers.</returns>
        public FindEmbeddingProvidersResult FindEmbeddingProviders(CommandOptions options)
        {
            return FindEmbeddingProvidersAsync(options, true).ResultSync();
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
        /// Asynchronously finds and returns available embedding providers using the specified command options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>
        /// A task that resolves to a <see cref="FindEmbeddingProvidersResult"/> containing the discovered providers.
        /// </returns>
        public Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(CommandOptions options)
        {
            return FindEmbeddingProvidersAsync(options, false);
        }

        internal async Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(CommandOptions options, bool runSynchronously)
        {
            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new DatabaseAdminTimeoutManager())
                .WithPayload(new { findEmbeddingProviders = new { } });

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
        /// Finds and returns available reranking providers using the specified options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>A <see cref="FindRerankingProvidersResult"/> containing the discovered providers.</returns>
        public FindRerankingProvidersResult FindRerankingProviders(FindRerankingProvidersCommandOptions options)
        {
            return FindRerankingProvidersAsync(options, true).ResultSync();
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
        /// Asynchronously finds and returns available reranking providers using the specified options.
        /// </summary>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <returns>
        /// A task that resolves to a <see cref="FindRerankingProvidersResult"/> containing the discovered providers.
        /// </returns>
        public Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersCommandOptions options)
        {
            return FindRerankingProvidersAsync(options, false);
        }

        internal async Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersCommandOptions options, bool runSynchronously)
        {
            if (options == null)
            {
                options = new FindRerankingProvidersCommandOptions();
            }
            options.DeserializeToObjectDictionary = true;
            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new DatabaseAdminTimeoutManager())
                .WithPayload(new
                {
                    findRerankingProviders = new
                    {
                        options = new
                        {
                            filterModelStatus = options.StatusString
                        }
                    }
                });

            var response = await command
                .RunAsyncReturnStatus<FindRerankingProvidersResult>(runSynchronously)
                .ConfigureAwait(false);

            return response.Result;
        }

        private Command CreateCommand()
        {
            return new Command(_database.Client, _optionsTree, new EmbeddingCommandUrlBuilder(_database));
        }
    }

    internal class FindKeyspacesResult
    {
        [JsonPropertyName("keyspaces")]
        public List<string> Keyspaces { get; set; }
    }
}
