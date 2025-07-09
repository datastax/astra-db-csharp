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
using DataStax.AstraDB.DataApi.Core.Results;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Admin;

public interface IDatabaseAdmin
{
    IEnumerable<string> ListKeyspaces();
    Task<IEnumerable<string>> ListKeyspacesAsync();
    FindEmbeddingProvidersResult FindEmbeddingProviders();
    Database GetDatabase();
    void CreateKeyspace(string keyspace);
    void CreateKeyspace(string keyspace, CommandOptions options);
    void CreateKeyspace(string keyspace, bool updateDBKeyspace);
    void CreateKeyspace(string keyspace, bool updateDBKeyspace, CommandOptions options);
    void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion);
    void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options);
    Task CreateKeyspaceAsync(string keyspace);
    Task CreateKeyspaceAsync(string keyspace, CommandOptions options);
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace);
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, CommandOptions options);
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion);
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options);
    void DropKeyspace(string keyspace);
    void DropKeyspace(string keyspace, CommandOptions options);
    void DropKeyspace(string keyspace, bool waitForCompletion);
    void DropKeyspace(string keyspace, bool waitForCompletion, CommandOptions options);
    Task DropKeyspaceAsync(string keyspace);
    Task DropKeyspaceAsync(string keyspace, CommandOptions options);
    Task DropKeyspaceAsync(string keyspace, bool waitForCompletion);
    Task DropKeyspaceAsync(string keyspace, bool waitForCompletion, CommandOptions options);
    bool DoesKeyspaceExist(string keyspace);
    Task<bool> DoesKeyspaceExistAsync(string keyspace);
}
