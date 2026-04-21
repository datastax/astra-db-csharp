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
using System.Collections.Generic;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Specifies vectorize options for a collection.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class CollectionVectorizeAttribute : BaseVectorizeAttribute
{
    /// <summary>The similarity metric to use for vector comparisons.</summary>
    public SimilarityMetric? Metric { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="CollectionVectorizeAttribute"/> with default settings.
    /// </summary>
    public CollectionVectorizeAttribute() { }

    /// <summary>
    /// Initializes a new instance with the specified provider and model name and optionally other settings.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="dimension">Optional: The number of dimensions for the generated vector.</param>
    /// <param name="authenticationPairs">Optional: Key-value pairs for authenticating with the embedding service.</param>
    /// <param name="parameterPairs">Optional: Additional key-value parameter pairs for the embedding service.</param>
    public CollectionVectorizeAttribute(
        string provider,
        string modelName,
        int dimension = -1,
        string[] authenticationPairs = null,
        object[] parameterPairs = null)
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension == -1 ? null : dimension;
        AuthenticationPairs = authenticationPairs ?? Array.Empty<string>();
        ParameterPairs = parameterPairs ?? Array.Empty<object>();
    }

    /// <summary>
    /// Initializes a new instance with the specified provider and model name and optionally other settings.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    /// <param name="dimension">Optional: The number of dimensions for the generated vector.</param>
    /// <param name="authenticationPairs">Optional: Key-value pairs for authenticating with the embedding service.</param>
    /// <param name="parameterPairs">Optional: Additional key-value parameter pairs for the embedding service.</param>
    public CollectionVectorizeAttribute(
        string provider,
        string modelName,
        SimilarityMetric metric,
        int dimension = -1,
        string[] authenticationPairs = null,
        object[] parameterPairs = null)
    {
        Provider = provider;
        ModelName = modelName;
        Metric = metric;
        Dimension = dimension == -1 ? null : dimension;
        AuthenticationPairs = authenticationPairs ?? Array.Empty<string>();
        ParameterPairs = parameterPairs ?? Array.Empty<object>();
    }

}
