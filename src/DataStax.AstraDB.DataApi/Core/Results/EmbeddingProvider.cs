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
/// A provider for embeddings.
/// </summary>
public class EmbeddingProvider
{
    public const string AuthenticationMethodNone = "NONE";
    public const string AuthenticationMethodSharedSecret = "SHARED_SECRET";
    public const string AuthenticationMethodHeader = "HEADER";

    public string DisplayName { get; set; }
    public string Url { get; set; }
    public Dictionary<string, AuthenticationMethod> SupportedAuthentication { get; set; }
    public List<Parameter> Parameters { get; set; }
    public List<Model> Models { get; set; }

    public class Model
    {
        public string Name { get; set; }
        public int VectorDimension { get; set; }
        public List<Parameter> Parameters { get; set; }
    }

    public class AuthenticationMethod
    {
        public bool Enabled { get; set; }
        public List<Token> Tokens { get; set; }
    }

    public class Token
    {
        public string Forwarded { get; set; }
        public string Accepted { get; set; }
    }

    public class Parameter
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public bool Required { get; set; }
        public string DefaultValue { get; set; }
        public Validation Validation { get; set; }
        public string Help { get; set; }
        public string DisplayName { get; set; }
        public string Hint { get; set; }
    }

    public class Validation
    {
        public List<int> NumericRange { get; set; }
    }
}