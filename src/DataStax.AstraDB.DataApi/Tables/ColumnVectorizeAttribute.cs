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

using System;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Marks a column to use automatic vectorization (embedding generation) via a configured embedding service.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class ColumnVectorizeAttribute : Attribute
{
    /// <summary>
    /// The number of dimensions for the generated vector. If not specified, the service default is used.
    /// </summary>
    public int? Dimension { get; set; }

    /// <summary>
    /// The name of the embedding service provider.
    /// </summary>
    public string ServiceProvider { get; set; }

    /// <summary>
    /// The model name to use for embedding generation.
    /// </summary>
    public string ServiceModelName { get; set; }

    /// <summary>
    /// Key-value pairs used for authenticating with the embedding service, supplied as alternating key and value strings.
    /// </summary>
    public string[] AuthenticationPairs { get; set; }

    /// <summary>
    /// Additional key-value parameter pairs for the embedding service, supplied as alternating key and value strings.
    /// </summary>
    public string[] ParameterPairs { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="ColumnVectorizeAttribute"/> with the specified embedding service configuration.
    /// </summary>
    public ColumnVectorizeAttribute(
        string serviceProvider,
        string serviceModelName,
        int dimension = -1,
        string[] authenticationPairs = null,
        string[] parameterPairs = null
    )
    {
        ServiceProvider = serviceProvider;
        ServiceModelName = serviceModelName;
        Dimension = dimension == -1 ? null : dimension;
        AuthenticationPairs = authenticationPairs ?? Array.Empty<string>();
        ParameterPairs = parameterPairs ?? Array.Empty<string>();
    }
}
