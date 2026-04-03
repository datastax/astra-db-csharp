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
public class VectorizeOptionsAttribute : Attribute
{
    /// <summary>The number of dimensions for the vector.</summary>
    public int Dimension { get; set; } = -1;

    /// <summary>The similarity metric to use for vector comparisons.</summary>
    public SimilarityMetric Metric { get; set; } = SimilarityMetric.Cosine;

    /// <summary>The name of the embedding service provider.</summary>
    public string Provider { get; set; }

    /// <summary>The model name to use for embedding generation.</summary>
    public string ModelName { get; set; }

    /// <summary>
    /// Key-value pairs used for authenticating with the embedding service, supplied as alternating key and value strings.
    /// </summary>
    public string[] AuthenticationPairs { get; set; }

    /// <summary>
    /// Additional key-value parameter pairs for the embedding service, supplied as alternating key and value strings.
    /// </summary>
    public string[] ParameterPairs { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="VectorizeOptionsAttribute"/> with default settings.
    /// </summary>
    public VectorizeOptionsAttribute() { }

    internal Dictionary<string, string> GetAuthentication()
    {
        return PairsToDict(AuthenticationPairs);
    }

    internal Dictionary<string, string> GetParameters()
    {
        return PairsToDict(ParameterPairs);
    }

    private static Dictionary<string, string> PairsToDict(string[] pairs)
    {
        if (pairs == null || pairs.Length == 0)
            return null;

        var dict = new Dictionary<string, string>();
        for (int i = 0; i + 1 < pairs.Length; i += 2)
            dict[pairs[i]] = pairs[i + 1];
        return dict.Count > 0 ? dict : null;
    }
}
