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

using System.Collections.Generic;

namespace DataStax.AstraDB.DataApi.Core.Results;

/// <summary>
/// Describes an embedding provider (such as OpenAI or Cohere) that can generate vector embeddings for use with the Data API.
/// </summary>
public class EmbeddingProvider
{
    /// <summary>
    /// Constant representing no authentication required.
    /// </summary>
    public const string AuthenticationMethodNone = "NONE";

    /// <summary>
    /// Constant representing shared-secret authentication.
    /// </summary>
    public const string AuthenticationMethodSharedSecret = "SHARED_SECRET";

    /// <summary>
    /// Constant representing header-based authentication.
    /// </summary>
    public const string AuthenticationMethodHeader = "HEADER";

    /// <summary>
    /// The human-readable display name of the embedding provider.
    /// </summary>
    public string DisplayName { get; set; }

    /// <summary>
    /// The base URL for the embedding provider's API endpoint.
    /// </summary>
    public string Url { get; set; }

    /// <summary>
    /// The authentication methods supported by this provider, keyed by method name.
    /// </summary>
    public Dictionary<string, AuthenticationMethod> SupportedAuthentication { get; set; }

    /// <summary>
    /// Configuration parameters accepted by this embedding provider.
    /// </summary>
    public List<Parameter> Parameters { get; set; }

    /// <summary>
    /// The embedding models available from this provider.
    /// </summary>
    public List<Model> Models { get; set; }

    /// <summary>
    /// Describes an embedding model offered by an <see cref="EmbeddingProvider"/>.
    /// </summary>
    public class Model
    {
        /// <summary>
        /// The name of the model.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The number of dimensions in the vectors produced by this model.
        /// </summary>
        public int VectorDimension { get; set; }

        /// <summary>
        /// Configuration parameters specific to this model.
        /// </summary>
        public List<Parameter> Parameters { get; set; }
    }

    /// <summary>
    /// Describes an authentication method supported by an <see cref="EmbeddingProvider"/>.
    /// </summary>
    public class AuthenticationMethod
    {
        /// <summary>
        /// Indicates whether this authentication method is enabled.
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// The tokens used for this authentication method.
        /// </summary>
        public List<Token> Tokens { get; set; }
    }

    /// <summary>
    /// Describes a token used in an <see cref="AuthenticationMethod"/>.
    /// </summary>
    public class Token
    {
        /// <summary>
        /// The token name as forwarded to the embedding provider.
        /// </summary>
        public string Forwarded { get; set; }

        /// <summary>
        /// The token name as accepted in the Data API request.
        /// </summary>
        public string Accepted { get; set; }
    }

    /// <summary>
    /// Describes a configuration parameter for an <see cref="EmbeddingProvider"/> or one of its models.
    /// </summary>
    public class Parameter
    {
        /// <summary>
        /// The parameter name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The data type of the parameter.
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Indicates whether this parameter is required.
        /// </summary>
        public bool Required { get; set; }

        /// <summary>
        /// The default value for this parameter, if any.
        /// </summary>
        public string DefaultValue { get; set; }

        /// <summary>
        /// Validation constraints for this parameter.
        /// </summary>
        public Dictionary<string, List<int>> Validation { get; set; }

        /// <summary>
        /// Help text describing the purpose of the parameter.
        /// </summary>
        public string Help { get; set; }

        /// <summary>
        /// The human-readable display name of the parameter.
        /// </summary>
        public string DisplayName { get; set; }

        /// <summary>
        /// A hint shown in UI contexts to assist with filling in this parameter.
        /// </summary>
        public string Hint { get; set; }
    }

}