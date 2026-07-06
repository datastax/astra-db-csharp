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

namespace DataStax.AstraDB.DataApi.IntegrationTests;

/// <summary>
/// Static configuration on the embedding provider for all vectorize-related integration testing.
/// </summary>
public static class EmbeddingProviderSwitcher {

    /// <summary>
    /// Embedding provider to use.
    /// </summary>
    public const string Provider = "voyageAI";
    /// <summary>
    /// Embedding model to use.
    /// </summary>
    public const string ModelName = "voyage-2";

    /// <summary>
    /// A dimension value compatible with the provider/model choice.
    /// </summary>
    public const int Dimension = 1024;

    /// <summary>
    /// Name of the secret in Astra scoped to the DB being used to test.
    /// </summary>
    public const string KMSSecretName = "SHARED_SECRET_EMBEDDING_API_KEY_VOYAGEAI";
    /// <summary>
    /// Name of the environment variable containing the embedding API key.
    /// </summary>
    public const string SecretEnvironmentVariableName = "HEADER_EMBEDDING_API_KEY_VOYAGEAI";
}
