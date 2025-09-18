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
using System.Text.Json;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Specifies options for creating lexical analysis based on a property
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class LexicalOptionsAttribute : Attribute
{
    public string TokenizerName { get; set; } = "standard";
    public string[] Filters { get; set; } = new string[0];
    public string[] CharacterFilters { get; set; } = new string[0];

    public string TokenizerArgumentsJson { get; set; }

    public LexicalOptionsAttribute() { }

    internal Dictionary<string, object> GetArguments()
    {
        if (string.IsNullOrEmpty(TokenizerArgumentsJson) || TokenizerArgumentsJson == "{}")
            return new Dictionary<string, object>();

        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, object>>(TokenizerArgumentsJson);
        }
        catch
        {
            return new Dictionary<string, object>();
        }
    }
}


