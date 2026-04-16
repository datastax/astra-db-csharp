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

using DataStax.AstraDB.DataApi.Core;
using System;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Marks a column to use automatic vectorization (embedding generation) via a configured embedding service.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class ColumnVectorizeAttribute : BaseVectorizeAttribute
{
    /// <summary>
    /// Initializes a new instance of <see cref="ColumnVectorizeAttribute"/> with the specified embedding service configuration.
    /// </summary>
    public ColumnVectorizeAttribute(
        string provider,
        string modelName,
        int dimension = -1,
        string[] authenticationPairs = null,
        object[] parameterPairs = null
    )
    {
        Provider = provider;
        ModelName = modelName;
        Dimension = dimension == -1 ? null : dimension;
        AuthenticationPairs = authenticationPairs ?? Array.Empty<string>();
        ParameterPairs = parameterPairs ?? Array.Empty<object>();
    }
}
