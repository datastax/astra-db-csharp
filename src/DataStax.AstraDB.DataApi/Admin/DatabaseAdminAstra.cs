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
        private static readonly CommandOptions _devOpsAPIOptions = new CommandOptions { SerializeDateAsDollarDate = false };

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
        /// The database Guid (as a string).
        /// </summary>
        public string Id => _id.ToString();

        /// <inheritdoc/>
        public Database GetDatabase(GetDatabaseOptions options = null)
        {
            return GetDatabase(null, options);
        }

        /// <inheritdoc/>
        public Database GetDatabase(string token, GetDatabaseOptions options = null)
        {
            var baseCommandOptions = CommandOptions.Merge(_optionsTree);
            var newCommandOptions = DatabaseCommandOptions.BinaryMerge(
                DatabaseCommandOptions.FromCommandOptions(baseCommandOptions),
                options
            );
            if (token != null)
            {
                newCommandOptions.Token = token;
            }
            return _client.GetDatabase(_database.APIEndpoint, newCommandOptions);
        }

        /// <summary>
        /// Gets the API endpoint URL for the associated database.
        /// </summary>
        /// <returns>The API endpoint as a string.</returns>
        /// <example>
        /// <code>
        /// string endpoint = admin.GetAPIEndpoint();
        /// </code>
        /// </example>
        public string GetAPIEndpoint()
        {
            return _database.APIEndpoint;
        }

        /// <inheritdoc/>
        public IEnumerable<string> ListKeyspaces(ListKeyspacesOptions options = null)
        {
            return ListKeyspacesAsync(true, options).ResultSync();
        }

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListKeyspacesAsync(ListKeyspacesOptions options = null)
        {
            return ListKeyspacesAsync(false, options);
        }

        internal async Task<IEnumerable<string>> ListKeyspacesAsync(bool runSynchronously, ListKeyspacesOptions options)
        {
            var databaseInfo = await _client.GetAstraDatabasesAdmin(
                GetAstraDatabasesAdminOptions.FromCommandOptions(_adminOptions)
            ).GetDatabaseInfoAsync(_id.ToString(), options, runSynchronously);
            return databaseInfo.Keyspaces;
        }

        /// <inheritdoc/>
        public void CreateKeyspace(string keyspace, CreateKeyspaceOptions options = null)
        {
            CreateKeyspaceAsync(keyspace, options, true).ResultSync();
        }

        /// <inheritdoc/>
        public Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceOptions options = null)
        {
            return CreateKeyspaceAsync(keyspace, options, false);
        }

        internal async Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceOptions options, bool runSynchronously)
        {
            options ??= new CreateKeyspaceOptions();
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
                    await Wait.WaitForProcess(() => DoesKeyspaceExistAsync(
                        keyspace,
                        DoesKeyspaceExistOptions.FromCommandOptions(options),
                        runSynchronously
                    )).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to create keyspace {keyspace} within the allotted time", e);
                }
            }
        }

        /// <inheritdoc/>
        public void DropKeyspace(string keyspace, DropKeyspaceOptions options = null)
        {
            DropKeyspaceAsync(keyspace, options, true).ResultSync();
        }

        /// <inheritdoc/>
        public Task DropKeyspaceAsync(string keyspace, DropKeyspaceOptions options = null)
        {
            return DropKeyspaceAsync(keyspace, options, false);
        }

        internal async Task DropKeyspaceAsync(string keyspace, DropKeyspaceOptions options, bool runSynchronously)
        {
            options ??= new DropKeyspaceOptions();
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
                    await Wait.WaitForProcess(
                        async () => !await DoesKeyspaceExistAsync(
                            keyspace,
                            DoesKeyspaceExistOptions.FromCommandOptions(options),
                            runSynchronously
                        ).ConfigureAwait(false)
                    ).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to drop keyspace {keyspace} within the allotted time", e);
                }
            }
        }

        /// <inheritdoc/>
        public bool DoesKeyspaceExist(string keyspace, DoesKeyspaceExistOptions options = null)
        {
            return DoesKeyspaceExistAsync(keyspace, options, true).ResultSync();
        }

        /// <inheritdoc/>
        public Task<bool> DoesKeyspaceExistAsync(string keyspace, DoesKeyspaceExistOptions options = null)
        {
            return DoesKeyspaceExistAsync(keyspace, options, false);
        }

        internal async Task<bool> DoesKeyspaceExistAsync(string keyspace, DoesKeyspaceExistOptions options, bool runSynchronously)
        {
            Guard.NotNullOrEmpty(keyspace, nameof(keyspace));
            var keyspaces = await ListKeyspacesAsync(runSynchronously, options).ConfigureAwait(false);
            return keyspaces.Contains(keyspace);
        }

        /// <inheritdoc/>
        public FindEmbeddingProvidersResult FindEmbeddingProviders(FindEmbeddingProvidersOptions options = null)
        {
            return FindEmbeddingProvidersAsync(options, true).ResultSync();
        }

        /// <inheritdoc/>
        public Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(FindEmbeddingProvidersOptions options = null)
        {
            return FindEmbeddingProvidersAsync(options, false);
        }

        internal async Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(FindEmbeddingProvidersOptions options, bool runSynchronously)
        {

            if (options == null){
                options = new FindEmbeddingProvidersOptions {};
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

            var command = CreateCommandEmbedding()
                .AddCommandOptions(options)
                .WithTimeoutManager(new DatabaseAdminTimeoutManager())
                .WithPayload(new { findEmbeddingProviders = epPayload });

            var response = await command
                .RunAsyncReturnStatus<FindEmbeddingProvidersResult>(runSynchronously)
                .ConfigureAwait(false);

            return response.Result;
        }

        /// <inheritdoc/>
        public FindRerankingProvidersResult FindRerankingProviders(FindRerankingProvidersOptions options = null)
        {
            return FindRerankingProvidersAsync(options, true).ResultSync();
        }
        
        /// <inheritdoc/>
        public Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersOptions options = null)
        {
            return FindRerankingProvidersAsync(options, false);
        }
        
        internal async Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersOptions options, bool runSynchronously)
        {

            if (options == null){
                options = new FindRerankingProvidersOptions {};
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
            var options = _optionsTree.Concat(new[] { _devOpsAPIOptions }).ToArray();
            return new Command(_database.Client, options, new AdminCommandUrlBuilder());
        }

        private Command CreateCommandEmbedding()
        {
            return new Command(_database.Client, _optionsTree, new EmbeddingCommandUrlBuilder(_database));
        }
    }
}
