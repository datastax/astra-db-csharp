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
    public class DatabaseAdminDataAPI : IDatabaseAdmin
    {
        private readonly Database _database;
        private readonly CommandOptions _adminOptions;
        private readonly DataAPIClient _client;
        private CommandOptions[] _optionsTree => new CommandOptions[] { _client.ClientOptions, _adminOptions };

        internal DatabaseAdminDataAPI(Database database, DataAPIClient client, CommandOptions adminOptions)
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
            return _client.GetDatabase(_database.APIEndpoint, new DatabaseCommandOptions { Keyspace = keyspace });
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
            return _client.GetDatabase(_database.APIEndpoint, new DatabaseCommandOptions { Keyspace = keyspace, Token = userToken });
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
        /// <remarks>
        /// This method, by default, will wait for the operation to complete on the server side.
        /// Use the options' waitForCompletion attribute to control this behaviour.
        /// </remarks>
        public void CreateKeyspace(string keyspace)
        {
            CreateKeyspace(keyspace, null, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, CreateKeyspaceCommandOptions)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, CreateKeyspaceCommandOptions)"/>
        public void CreateKeyspace(string keyspace, CreateKeyspaceCommandOptions options)
        {
            CreateKeyspace(keyspace, options, null);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, CreateKeyspaceCommandOptions, IDictionary{string,object})"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, CreateKeyspaceCommandOptions, IDictionary{string,object})"/>
        public void CreateKeyspace(string keyspace, CreateKeyspaceCommandOptions options, IDictionary<string,object> replicationOptions)
        {
            CreateKeyspaceAsync(keyspace, options, replicationOptions, true).ResultSync();
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
            return CreateKeyspaceAsync(keyspace, null, null);
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
            return CreateKeyspaceAsync(keyspace, options, null);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <param name="replicationOptions">Optional replication settings for the keyspace, e.g. {"class": "SimpleStrategy", "replication_factor": 3}.</param>
        /// <example>
        /// <code>
        /// var replicationSettings = new Dictionary&lt;string, object&gt; { ["class"] = "SimpleStrategy", ["replication_factor"] = 3 };
        /// await admin.CreateKeyspaceAsync("myKeyspace", options, replicationSettings);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceCommandOptions options, IDictionary<string,object> replicationOptions)
        {
            return CreateKeyspaceAsync(keyspace, options, replicationOptions, false);
        }

        internal async Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceCommandOptions options, IDictionary<string,object> replicationOptions, bool runSynchronously)
        {
            options ??= new CreateKeyspaceCommandOptions();
            options.IncludeKeyspaceInUrl = false;
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));

            var createKeyspacePayload = new Dictionary<string, object>
            {
                ["name"] = keyspace
            };
            if (replicationOptions != null)
            {
                createKeyspacePayload["options"] = new Dictionary<string, object>
                {
                    ["replication"] = replicationOptions
                };
            }

            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .WithPayload(new
                {
                    createKeyspace = createKeyspacePayload
                });

            await command
                .RunAsyncReturnStatus<object>(runSynchronously)
                .ConfigureAwait(false);

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
        /// Synchronous version of <see cref="DropKeyspaceAsync(string)"/>.
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
        /// Synchronous version of <see cref="DropKeyspaceAsync(string, BlockingCommandOptions)"/>.
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
            return DropKeyspaceAsync(keyspace, null, false);
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
        public FindEmbeddingProvidersResult FindEmbeddingProviders(FindEmbeddingProvidersCommandOptions options)
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
                        filterModelStatus = options.FilterModelStatus.Value.ToAPIString()
                    }
                };

            var command = CreateCommand()
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
                        filterModelStatus = options.FilterModelStatus.Value.ToAPIString()
                    }
                };

            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new DatabaseAdminTimeoutManager())
                .WithPayload(new { findRerankingProviders = epPayload });

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
        /// <summary>
        /// The collection of keyspace names present in the database.
        /// </summary>
        [JsonPropertyName("keyspaces")]
        public List<string> Keyspaces { get; set; }
    }
}
