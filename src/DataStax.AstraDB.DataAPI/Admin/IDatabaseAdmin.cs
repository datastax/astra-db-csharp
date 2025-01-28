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

namespace DataStax.AstraDB.DataApi.Admin;

public interface IDatabaseAdmin
{
    IEnumerable<string> ListKeyspaceNames();
    FindEmbeddingProvidersResult FindEmbeddingProviders();
    Task<IEnumerable<string>> ListKeyspaceNamesAsync();
    Database GetDatabase(string keyspace);
    Database GetDatabase(string keyspace, string userToken);
    Database GetDatabase();
    void DropKeyspace(string keyspace);
    void DropKeyspace(string keyspace, CommandOptions options);
    Task DropKeyspaceAsync(string keyspace);
    void CreateKeyspace(string keyspace, bool updateDBKeyspace);
    void CreateKeyspace(string keyspace);
    Task CreateKeyspaceAsync(string keyspace);
    bool KeyspaceExists(string keyspace);
}
