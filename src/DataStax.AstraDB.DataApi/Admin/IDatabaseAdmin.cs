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
    /// <summary>
    /// Returns a <see cref="Database"/> instance for non-admin usage.
    /// </summary>
    /// <param name="options">options for the returned database: useful to override timeouts and other behavior.</param>
    /// <returns>A <see cref="Database"/> instance representing the current database.</returns>
    /// <example>
    /// <code>
    /// var database = admin.GetDatabase("myToken");
    /// </code>
    /// </example>
    Database GetDatabase(GetDatabaseOptions options = null);

    /// <inheritdoc cref="GetDatabase(GetDatabaseOptions)"/>
    /// <param name="token">The token that will be used by the database. Omit to keep using the current token.</param>
    /// <param name="options">options for the returned database: useful to override timeouts and other behavior.</param>
    Database GetDatabase(string token, GetDatabaseOptions options = null);

    /// <summary>
    /// Synchronous version of <see cref="ListKeyspacesAsync(ListKeyspacesOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="ListKeyspacesAsync(ListKeyspacesOptions)"/>
    /// <example>
    /// <code>
    /// IEnumerable&lt;string&gt; keyspaces = admin.ListKeyspaces();
    /// </code>
    /// </example>
    IEnumerable<string> ListKeyspaces(ListKeyspacesOptions options = null);
 
    /// <summary>
    /// Lists the names of all keyspaces in the database.
    /// </summary>
    /// <param name="options">Optional settings that influence request execution.</param>
    /// <returns>A collection of keyspace names.</returns>
    /// <example>
    /// <code>
    /// var keyspaces = await admin.ListKeyspacesAsync();
    /// </code>
    /// </example>
    Task<IEnumerable<string>> ListKeyspacesAsync(ListKeyspacesOptions options = null);

    /// <summary>
    /// Synchronous version of <see cref="FindEmbeddingProvidersAsync(FindEmbeddingProvidersOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="FindEmbeddingProvidersAsync(FindEmbeddingProvidersOptions)"/>
    /// <example>
    /// <code>
    /// var providers = admin.FindEmbeddingProviders();
    /// </code>
    /// </example>
    FindEmbeddingProvidersResult FindEmbeddingProviders(FindEmbeddingProvidersOptions options = null);

    /// <summary>
    /// Finds and returns available embedding providers for the current database.
    /// </summary>
    /// <param name="options">Optional settings that influence request execution.</param>
    /// <returns>A <see cref="FindEmbeddingProvidersResult"/> containing the discovered providers.</returns>
    /// <example>
    /// <code>
    /// var providers = await admin.FindEmbeddingProvidersAsync();
    /// </code>
    /// </example>
    Task<FindEmbeddingProvidersResult> FindEmbeddingProvidersAsync(FindEmbeddingProvidersOptions options = null);

    /// <summary>
    /// Synchronous version of <see cref="FindRerankingProviders(FindRerankingProvidersOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="FindRerankingProviders(FindRerankingProvidersOptions)"/>
    /// <example>
    /// <code>
    /// var providers = admin.FindRerankingProviders();
    /// </code>
    /// </example>
    FindRerankingProvidersResult FindRerankingProviders(FindRerankingProvidersOptions options = null);

    /// <summary>
    /// Finds and returns available reranking providers for the current database.
    /// </summary>
    /// <param name="options">Optional settings that influence request execution.</param>
    /// <returns>A <see cref="FindRerankingProvidersResult"/> containing the discovered providers.</returns>
    /// <example>
    /// <code>
    /// var providers = await admin.FindRerankingProvidersAsync();
    /// </code>
    /// </example>
    Task<FindRerankingProvidersResult> FindRerankingProvidersAsync(FindRerankingProvidersOptions options = null);

    /// <summary>
    /// Synchronous version of <see cref="CreateKeyspaceAsync(string, CreateKeyspaceOptions)"/>
    /// </summary>
    /// <inheritdoc cref="CreateKeyspaceAsync(string, CreateKeyspaceOptions)"/>
    void CreateKeyspace(string keyspace, CreateKeyspaceOptions options = null);

    /// <summary>
    /// Creates a new keyspace with the specified name.
    /// </summary>
    /// <param name="keyspace">The name of the keyspace to create.</param>
    /// <param name="options">Optional settings that influence request execution.</param>
    /// <example>
    /// <code>
    /// await admin.CreateKeyspaceAsync("myKeyspace");
    /// </code>
    /// </example>
    /// <remarks>
    /// This method, by default, will wait for the operation to complete on the server side.
    /// Use the options' waitForCompletion attribute to control this behaviour.
    /// </remarks>
    Task CreateKeyspaceAsync(string keyspace, CreateKeyspaceOptions options = null);

    /// <summary>
    /// Synchronous version of <see cref="DropKeyspaceAsync(string, DropKeyspaceOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="DropKeyspaceAsync(string, DropKeyspaceOptions)"/>
    void DropKeyspace(string keyspace, DropKeyspaceOptions options = null);

    /// <summary>
    /// Drops the keyspace with the specified name.
    /// </summary>
    /// <param name="keyspace">The name of the keyspace to drop.</param>
    /// <param name="options">Optional settings that influence request execution.</param>
    /// <example>
    /// <code>
    /// await admin.DropKeyspaceAsync("myKeyspace", options);
    /// </code>
    /// </example>
    Task DropKeyspaceAsync(string keyspace, DropKeyspaceOptions options = null);

    /// <summary>
    /// Synchronous version of <see cref="DoesKeyspaceExistAsync(string, DoesKeyspaceExistOptions)"/>.
    /// </summary>
    /// <inheritdoc cref="DoesKeyspaceExistAsync(string, DoesKeyspaceExistOptions)"/>
    /// <example>
    /// <code>
    /// bool exists = admin.DoesKeyspaceExist("myKeyspace");
    /// </code>
    /// </example>
    bool DoesKeyspaceExist(string keyspace, DoesKeyspaceExistOptions options = null);

    /// <summary>
    /// Checks whether a keyspace with the specified name exists.
    /// </summary>
    /// <param name="keyspace">The name of the keyspace to check.</param>
    /// <param name="options">Optional settings that influence request execution.</param>
    /// <returns>A task that resolves to <c>true</c> if the keyspace exists; otherwise, <c>false</c>.</returns>
    /// <example>
    /// <code>
    /// bool exists = await admin.DoesKeyspaceExistAsync("myKeyspace");
    /// </code>
    /// </example>
    Task<bool> DoesKeyspaceExistAsync(string keyspace, DoesKeyspaceExistOptions options = null);

}
