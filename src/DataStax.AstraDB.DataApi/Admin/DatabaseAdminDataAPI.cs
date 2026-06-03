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

        /// <inheritdoc/>
        public Database GetDatabase(GetDatabaseOptions options = null)
        {
            return GetDatabase(null, options);
        }

        /// <inheritdoc/>
        public Database GetDatabase(string token, GetDatabaseOptions options = null)
        {
            var baseCommandOptions = CommandOptions.Merge(_optionsTree);
            var newCommandOptions = GetDatabaseOptions.BinaryMerge(
                GetDatabaseOptions.FromCommandOptions(baseCommandOptions),
                options
            );
            if (token != null)
            {
                newCommandOptions.Token = token;
            }
            return _client.GetDatabase(_database.APIEndpoint, newCommandOptions);
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
            var command = CreateCommand()
                .AddCommandOptions(options)
                .WithTimeoutManager(new KeyspaceAdminTimeoutManager())
                .WithPayload(new { findKeyspaces = new { } });

            var response = await command
                .RunAsyncReturnStatus<FindKeyspacesResult>(runSynchronously)
                .ConfigureAwait(false);

            return response.Result?.Keyspaces ?? new List<string>();
        }

        /// <inheritdoc/>
        public void CreateKeyspace(string keyspace, CreateKeyspaceOptions options = null)
        {
            CreateKeyspace(keyspace, null, options);
        }

        /// <summary>
        /// Synchronous version of <see cref="CreateKeyspaceAsync(string, IDictionary{string,object}, CreateKeyspaceOptions)"/>.
        /// </summary>
        /// <inheritdoc cref="CreateKeyspaceAsync(string, IDictionary{string,object}, CreateKeyspaceOptions)"/>
        public void CreateKeyspace(string keyspace, IDictionary<string,object> replicationOptions, CreateKeyspaceOptions options = null)
        {
            CreateKeyspaceAsync(keyspace, replicationOptions, options, true).ResultSync();
        }

        /// <inheritdoc/>
        public Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceOptions options = null)
        {
            return CreateKeyspaceAsync(keyspace, null, options);
        }

        /// <inheritdoc cref="CreateKeyspaceAsync(string, CreateKeyspaceOptions)"/>
        /// <param name="keyspace">The name of the keyspace to create.</param>
        /// <param name="replicationOptions">Optional replication settings for the keyspace, e.g. {"class": "SimpleStrategy", "replication_factor": 3}.</param>
        /// <param name="options">Optional settings that influence request execution.</param>
        /// <example>
        /// <code>
        /// var replicationOptions = new Dictionary&lt;string, object&gt; { ["class"] = "SimpleStrategy", ["replication_factor"] = 3 };
        /// await admin.CreateKeyspaceAsync("myKeyspace", replicationOptions, options);
        /// </code>
        /// </example>
        public Task CreateKeyspaceAsync(string keyspace, IDictionary<string,object> replicationOptions, CreateKeyspaceOptions options = null)
        {
            return CreateKeyspaceAsync(keyspace, replicationOptions, options, false);
        }

        internal async Task CreateKeyspaceAsync(string keyspace, IDictionary<string,object> replicationOptions, CreateKeyspaceOptions options, bool runSynchronously)
        {
            options ??= new CreateKeyspaceOptions();
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
                    await Wait.WaitForProcess(
                        () => DoesKeyspaceExistAsync(
                            keyspace,
                            DoesKeyspaceExistOptions.FromCommandOptions(options),
                            runSynchronously
                        )
                    ).ConfigureAwait(false);
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
            return new System.Collections.Generic.HashSet<string>(keyspaces).Contains(keyspace);
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

            var command = CreateCommand()
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
