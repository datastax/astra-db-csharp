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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Command options specific to collections.
/// </summary>
public class GetCollectionOptions : DatabaseCommandOptions
{
    /// <summary>
    /// When specified, the client will send the X-Embedding-Api-Key header with the specified key to any underlying HTTP request that requires vectorize authentication.
    /// </summary>
    public string EmbeddingAPIKey
    {
        get
        {
            return AdditionalHeaders.TryGetValue("X-Embedding-Api-Key", out var value) ? value : null;
        }
        set
        {
            AdditionalHeaders["X-Embedding-Api-Key"] = value;
        }
    }

    /// <summary>
    /// When specified, the client will send the authentication parameters required for AWS embedding providers (Access ID and Secret ID)
    /// with each collection request, via HTTP headers.
    /// </summary>
    /// <example>
    /// <code>
    /// // When creating a collection:
    /// var collection = await Database.CreateCollectionAsync&lt;MyDocumentClass&gt;(
    ///     new CreateCollectionOptions() {
    ///         AWSEmbeddingAPIKey = new () { EmbeddingAccessId = "..." , EmbeddingSecretId = "..." }
    ///     }
    /// );
    /// // Similarly for getting a collection:
    /// var collection = Database.GetCollection&lt;MyDocumentClass&gt;(
    ///     new GetCollectionOptions() {
    ///         AWSEmbeddingAPIKey = new () { EmbeddingAccessId = "..." , EmbeddingSecretId = "..." }
    ///     }
    /// );
    /// </code>
    /// </example>
    public AWSEmbeddingAPIKeyDescriptor AWSEmbeddingAPIKey
    {
        get
        {
            var accessId = AdditionalHeaders.TryGetValue("X-Embedding-Access-Id", out var result_access) ? result_access : null;
            var secretId = AdditionalHeaders.TryGetValue("X-Embedding-Secret-Id", out var result_secret) ? result_secret : null;
            if (accessId != null && secretId != null){
                return new () {
                    EmbeddingAccessId = accessId,
                    EmbeddingSecretId = secretId
                };
            }
            else
            {
                return null;
            }
        }
        set
        {
            if ( value != null )
            {
                AdditionalHeaders["X-Embedding-Access-Id"] = value.EmbeddingAccessId;
                AdditionalHeaders["X-Embedding-Secret-Id"] = value.EmbeddingSecretId;
            }
        }
    }


    /// <summary>
    /// When specified, the client will send the Reranking-Api-Key header with the specified key to any underlying HTTP request that requires reranker authentication.
    /// </summary>
    public string RerankingAPIKey
    {
        get
        {
            return AdditionalHeaders.TryGetValue("Reranking-Api-Key", out var value) ? value : null;
        }
        set
        {
            AdditionalHeaders["Reranking-Api-Key"] = value;
        }
    }
}

/// <summary>
/// Specification for the authentication secrets required for AWS embedding providers.
/// </summary>
public class AWSEmbeddingAPIKeyDescriptor {
    /// <summary>
    /// The Access ID for the embedding service being accessed.
    /// </summary>
    public string EmbeddingAccessId { get; set; }
    /// <summary>
    /// The Secret ID for the embedding service being accessed.
    /// </summary>
    public string EmbeddingSecretId { get; set; }
}
