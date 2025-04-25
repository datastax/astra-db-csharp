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

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class ColumnVectorizeAttribute : Attribute
{
    public int Dimension { get; set; }
    public string ServiceProvider { get; set; }
    public string ServiceModelName { get; set; }
    public string[] AuthenticationPairs { get; set; }
    public string[] ParameterPairs { get; set; }

    public ColumnVectorizeAttribute(
        int dimension,
        string serviceProvider,
        string serviceModelName,
        string[] authenticationPairs = null,
        string[] parameterPairs = null
    )
    {
        Dimension = dimension;
        ServiceProvider = serviceProvider;
        ServiceModelName = serviceModelName;
        AuthenticationPairs = authenticationPairs ?? Array.Empty<string>();
        ParameterPairs = parameterPairs ?? Array.Empty<string>();
    }
}

