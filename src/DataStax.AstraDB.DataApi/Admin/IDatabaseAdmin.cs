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

/// <summary>
/// Defines administrative operations for a database, including keyspace management
/// and discovery of embedding and reranking providers.
/// </summary>
public interface IDatabaseAdmin
{
    /// <summary>Lists all keyspaces in the database.</summary>
    IEnumerable<string> ListKeyspaces();
    /// <summary>Lists all keyspaces in the database with the specified command options.</summary>
    IEnumerable<string> ListKeyspaces(CommandOptions options);
    /// <summary>Asynchronously lists all keyspaces in the database.</summary>
    Task<IEnumerable<string>> ListKeyspacesAsync();
    /// <summary>Asynchronously lists all keyspaces in the database with the specified command options.</summary>
    Task<IEnumerable<string>> ListKeyspacesAsync(CommandOptions options);
    /// <summary>Returns the available embedding providers for this database.</summary>
    FindEmbeddingProvidersResult FindEmbeddingProviders();
    /// <summary>Returns the available embedding providers for this database with the specified options.</summary>
    FindEmbeddingProvidersResult FindEmbeddingProviders(FindEmbeddingProvidersCommandOptions options);
    /// <summary>Asynchronously returns the available embedding providers for this database.</summary>
    Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync();
    /// <summary>Asynchronously returns the available embedding providers for this database with the specified options.</summary>
    Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(FindEmbeddingProvidersCommandOptions options);
    /// <summary>Returns the available reranking providers for this database.</summary>
    FindRerankingProvidersResult FindRerankingProviders();
    /// <summary>Returns the available reranking providers for this database with the specified options.</summary>
    FindRerankingProvidersResult FindRerankingProviders(FindRerankingProvidersCommandOptions options);
    /// <summary>Asynchronously returns the available reranking providers for this database.</summary>
    Task<FindRerankingProvidersResult> FindRerankingProvidersAsync();
    /// <summary>Asynchronously returns the available reranking providers for this database with the specified options.</summary>
    Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersCommandOptions options);
    /// <summary>Returns the <see cref="Database"/> instance associated with this admin.</summary>
    Database GetDatabase();
    /// <summary>Creates a new keyspace with the specified name.</summary>
    void CreateKeyspace(string keyspace);
    /// <summary>Creates a new keyspace with the specified name and command options.</summary>
    void CreateKeyspace(string keyspace, CommandOptions options);
    /// <summary>Creates a new keyspace, optionally updating the database's default keyspace.</summary>
    void CreateKeyspace(string keyspace, bool updateDBKeyspace);
    /// <summary>Creates a new keyspace, optionally updating the database's default keyspace, with command options.</summary>
    void CreateKeyspace(string keyspace, bool updateDBKeyspace, CommandOptions options);
    /// <summary>Creates a new keyspace, optionally updating the default keyspace and waiting for completion.</summary>
    void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion);
    /// <summary>Creates a new keyspace with all options specified.</summary>
    void CreateKeyspace(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options);
    /// <summary>Asynchronously creates a new keyspace with the specified name.</summary>
    Task CreateKeyspaceAsync(string keyspace);
    /// <summary>Asynchronously creates a new keyspace with the specified name and command options.</summary>
    Task CreateKeyspaceAsync(string keyspace, CommandOptions options);
    /// <summary>Asynchronously creates a new keyspace, optionally updating the database's default keyspace.</summary>
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace);
    /// <summary>Asynchronously creates a new keyspace, optionally updating the database's default keyspace, with command options.</summary>
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, CommandOptions options);
    /// <summary>Asynchronously creates a new keyspace, optionally updating the default keyspace and waiting for completion.</summary>
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion);
    /// <summary>Asynchronously creates a new keyspace with all options specified.</summary>
    Task CreateKeyspaceAsync(string keyspace, bool updateDBKeyspace, bool waitForCompletion, CommandOptions options);
    /// <summary>Drops the keyspace with the specified name.</summary>
    void DropKeyspace(string keyspace);
    /// <summary>Drops the keyspace with the specified name and command options.</summary>
    void DropKeyspace(string keyspace, CommandOptions options);
    /// <summary>Drops the keyspace, optionally waiting for the operation to complete.</summary>
    void DropKeyspace(string keyspace, bool waitForCompletion);
    /// <summary>Drops the keyspace, optionally waiting for completion, with command options.</summary>
    void DropKeyspace(string keyspace, bool waitForCompletion, CommandOptions options);
    /// <summary>Asynchronously drops the keyspace with the specified name.</summary>
    Task DropKeyspaceAsync(string keyspace);
    /// <summary>Asynchronously drops the keyspace with the specified name and command options.</summary>
    Task DropKeyspaceAsync(string keyspace, CommandOptions options);
    /// <summary>Asynchronously drops the keyspace, optionally waiting for the operation to complete.</summary>
    Task DropKeyspaceAsync(string keyspace, bool waitForCompletion);
    /// <summary>Asynchronously drops the keyspace, optionally waiting for completion, with command options.</summary>
    Task DropKeyspaceAsync(string keyspace, bool waitForCompletion, CommandOptions options);
    /// <summary>Returns <see langword="true"/> if the specified keyspace exists in the database.</summary>
    bool DoesKeyspaceExist(string keyspace);
    /// <summary>Asynchronously returns <see langword="true"/> if the specified keyspace exists in the database.</summary>
    Task<bool> DoesKeyspaceExistAsync(string keyspace);
}
