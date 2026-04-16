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
    /// Initializes a new instance with the specified provider and model name.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    public CollectionVectorizeAttribute(string provider, string modelName)
    {
        Provider = provider;
        ModelName = modelName;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, and dimension.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="dimension">The number of dimensions for the generated vector.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, int dimension)
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, dimension, and metric.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="dimension">The number of dimensions for the generated vector.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, int dimension, SimilarityMetric metric)
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension;
        Metric = metric;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, dimension, metric, and authentication.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="dimension">The number of dimensions for the generated vector.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    /// <param name="authenticationPairs">Key-value pairs for authenticating with the embedding service.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, int dimension, SimilarityMetric metric, string[] authenticationPairs)
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension;
        Metric = metric;
        AuthenticationPairs = authenticationPairs;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, dimension, metric, authentication, and parameters.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="dimension">The number of dimensions for the generated vector.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    /// <param name="authenticationPairs">Key-value pairs for authenticating with the embedding service.</param>
    /// <param name="parameterPairs">Additional key-value parameter pairs for the embedding service.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, int dimension, SimilarityMetric metric, string[] authenticationPairs, object[] parameterPairs)
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension;
        Metric = metric;
        AuthenticationPairs = authenticationPairs;
        ParameterPairs = parameterPairs;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, dimension, and authentication.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="dimension">The number of dimensions for the generated vector.</param>
    /// <param name="authenticationPairs">Key-value pairs for authenticating with the embedding service.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, int dimension, string[] authenticationPairs)
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension;
        AuthenticationPairs = authenticationPairs;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, dimension, authentication, and parameters.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="dimension">The number of dimensions for the generated vector.</param>
    /// <param name="authenticationPairs">Key-value pairs for authenticating with the embedding service.</param>
    /// <param name="parameterPairs">Additional key-value parameter pairs for the embedding service.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, int dimension, string[] authenticationPairs, object[] parameterPairs)
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension;
        AuthenticationPairs = authenticationPairs;
        ParameterPairs = parameterPairs;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, and metric.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, SimilarityMetric metric)
    {
        Provider = provider;
        ModelName = modelName;
        Metric = metric;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, metric, and authentication.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    /// <param name="authenticationPairs">Key-value pairs for authenticating with the embedding service.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, SimilarityMetric metric, string[] authenticationPairs)
    {
        Provider = provider;
        ModelName = modelName;
        Metric = metric;
        AuthenticationPairs = authenticationPairs;
    }

    /// <summary>
    /// Initializes a new instance with the specified provider, model name, metric, authentication, and parameters.
    /// </summary>
    /// <param name="provider">The name of the embedding service provider.</param>
    /// <param name="modelName">The model name to use for embedding generation.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    /// <param name="authenticationPairs">Key-value pairs for authenticating with the embedding service.</param>
    /// <param name="parameterPairs">Additional key-value parameter pairs for the embedding service.</param>
    public CollectionVectorizeAttribute(string provider, string modelName, SimilarityMetric metric, string[] authenticationPairs, object[] parameterPairs)
    {
        Provider = provider;
        ModelName = modelName;
        Metric = metric;
        AuthenticationPairs = authenticationPairs;
        ParameterPairs = parameterPairs;
    }

}
