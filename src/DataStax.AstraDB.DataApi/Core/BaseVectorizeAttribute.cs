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
/// Base class for vectorization attributes, providing common properties.
/// </summary>
public class BaseVectorizeAttribute : Attribute
{
    /// <summary>
    /// The number of dimensions for the generated vector. If not specified, the service default is used.
    /// </summary>
    public int? Dimension { get; set; } = null;

    /// <summary>
    /// The name of the embedding service provider.
    /// </summary>
    public string Provider { get; set; }

    /// <summary>
    /// The model name to use for embedding generation.
    /// </summary>
    public string ModelName { get; set; }

    /// <summary>
    /// Key-value pairs used for authenticating with the embedding service, supplied as alternating key and value strings.
    /// </summary>
    public string[] AuthenticationPairs { get; set; }

    /// <summary>
    /// Additional key-value parameter pairs for the embedding service, supplied as alternating key and value strings.
    /// </summary>
    public object[] ParameterPairs { get; set; }

    internal void Validate(string identifier)
    {
        if (ParameterPairs != null && ParameterPairs.Length > 0)
        {
            if (ParameterPairs.Length % 2 != 0)
            {
                throw new InvalidOperationException($"ParameterPairs for {identifier} must contain an even number of elements (string/object pairs).");
            }
            for (int i = 0; i < ParameterPairs.Length; i += 2)
            {
                if (ParameterPairs[i] is not string)
                {
                    throw new InvalidOperationException($"ParameterPairs for {identifier} must be pairs of string keys followed by object values. Index {i} is not a string).");
                }
            }
        }
    }

    internal Dictionary<string, string> GetAuthentication()
    {
        return PairsToDict(AuthenticationPairs);
    }

    internal Dictionary<string, object> GetParameters()
    {
        return PairsToDict(ParameterPairs);
    }

    private static Dictionary<string, T> PairsToDict<T>(T[] pairs)
    {
        if (pairs == null || pairs.Length == 0)
        {
            return null;
        }

        var dict = new Dictionary<string, T>();
        for (int i = 0; i + 1 < pairs.Length; i += 2)
        {
            dict[pairs[i] as string] = pairs[i + 1];
        }
        return dict.Count > 0 ? dict : null;
    }
}
