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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Specifies vector options for a collection.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class CollectionVectorAttribute : Attribute
{
    /// <summary>The number of dimensions for the vector.</summary>
    public int? Dimension { get; set; } = null;

    /// <summary>The similarity metric to use for vector comparisons.</summary>
    public SimilarityMetric Metric { get; set; } = SimilarityMetric.Cosine;

    /// <summary>
    /// Configures the index with the fastest settings for a given source of embeddings vectors.
    /// Example values include 'openai-v3-large', 'cohere-v3', 'bert'. Defaults to 'other' if not set.
    /// </summary>
    public string SourceModel { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="CollectionVectorAttribute"/> with default settings.
    /// </summary>
    public CollectionVectorAttribute() { }

    /// <summary>
    /// Initializes a new instance with the specified dimension.
    /// </summary>
    /// <param name="dimension">The number of dimensions for the vector.</param>
    public CollectionVectorAttribute(int dimension)
    {
        Dimension = dimension;
    }

    /// <summary>
    /// Initializes a new instance with the specified dimension and metric.
    /// </summary>
    /// <param name="dimension">The number of dimensions for the vector.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    public CollectionVectorAttribute(int dimension, SimilarityMetric metric)
    {
        Dimension = dimension;
        Metric = metric;
    }

    /// <summary>
    /// Initializes a new instance with the specified dimension, metric, and source model.
    /// </summary>
    /// <param name="dimension">The number of dimensions for the vector.</param>
    /// <param name="metric">The similarity metric to use for vector comparisons.</param>
    /// <param name="sourceModel">The source model for embeddings optimization.</param>
    public CollectionVectorAttribute(int dimension, SimilarityMetric metric, string sourceModel)
    {
        Dimension = dimension;
        Metric = metric;
        SourceModel = sourceModel;
    }
}
