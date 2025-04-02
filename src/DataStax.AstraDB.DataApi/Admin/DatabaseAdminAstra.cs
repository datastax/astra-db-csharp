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
    public class EmbeddingProviderInfo
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string Version { get; set; }
        public List<string> SupportedModels { get; set; }
    }

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

        public Database GetDatabase()
        {
            return _database;
        }

        public string GetApiEndpoint()
        {
            return _database.ApiEndpoint;
        }

        public IEnumerable<string> ListKeyspaceNames()
        {
            return ListKeyspaceNamesAsync(true, null).ResultSync();
        }

        public Task<IEnumerable<string>> ListKeyspaceNamesAsync()
        {
            return ListKeyspaceNamesAsync(false, null);
        }

        public IEnumerable<string> ListKeyspaceNames(CommandOptions options)
        {
            return ListKeyspaceNamesAsync(true, options).ResultSync();
        }

        public Task<IEnumerable<string>> ListKeyspaceNamesAsync(CommandOptions options)
        {
            return ListKeyspaceNamesAsync(false, options);
        }

        internal async Task<IEnumerable<string>> ListKeyspaceNamesAsync(bool runSynchronously, CommandOptions options)
        {
            var databaseInfo = await _client.GetAstraDatabasesAdmin().GetDatabaseInfoAsync(_id, options, runSynchronously);
            return databaseInfo.Info.Keyspaces;
        }

        public void CreateKeyspace(string keyspace)
        {
            CreateKeyspaceAsync(keyspace, false, null, true).ResultSync();
        }

        public void CreateKeyspace(string keyspace, bool updateDBKeyspace)
        {
            CreateKeyspaceAsync(keyspace, updateDBKeyspace, null, true).ResultSync();
        }

        public void CreateKeyspace(string keyspace, CommandOptions options)
        {
            CreateKeyspaceAsync(keyspace, false, options, true).ResultSync();
        }

        public void CreateKeyspace(string keyspace, bool updateDBKeyspace, CommandOptions options)
        {
            CreateKeyspaceAsync(keyspace, updateDBKeyspace, options, true).ResultSync();
        }

        public Task CreateKeyspaceAsync(string keyspace)
        {
            return CreateKeyspaceAsync(keyspace, false, null, false);
        }

        public Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace)
        {
            return CreateKeyspaceAsync(keyspace, updateDBKeyspace, null, false);
        }

        public Task CreateKeyspaceAsync(string keyspace, CommandOptions options)
        {
            return CreateKeyspaceAsync(keyspace, false, options, false);
        }

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

        public void DropKeyspace(string keyspace)
        {
            DropKeyspaceAsync(keyspace, null, true).ResultSync();
        }

        public Task DropKeyspaceAsync(string keyspace)
        {
            return DropKeyspaceAsync(keyspace, null, false);
        }
        public void DropKeyspace(string keyspace, CommandOptions options)
        {
            DropKeyspaceAsync(keyspace, options, true).ResultSync();
        }

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

        public bool KeyspaceExists(string keyspace)
        {
            return KeyspaceExistsAsync(keyspace, null, true).ResultSync();
        }

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

        public FindEmbeddingProvidersResult FindEmbeddingProviders()
        {
            return FindEmbeddingProvidersAsync(null, true).ResultSync();
        }

        public Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync()
        {
            return FindEmbeddingProvidersAsync(null, false);
        }

        public FindEmbeddingProvidersResult FindEmbeddingProviders(CommandOptions options)
        {
            return FindEmbeddingProvidersAsync(options, true).ResultSync();
        }

        public Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(CommandOptions options)
        {
            return FindEmbeddingProvidersAsync(options, false);
        }

        public class FindEmbeddingProvidersResponse
        {
            public Status status { get; set; }
        }

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
