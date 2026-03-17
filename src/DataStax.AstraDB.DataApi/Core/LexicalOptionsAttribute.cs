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
    /// <summary>The name of the tokenizer to use. Defaults to "standard".</summary>
    public string TokenizerName { get; set; } = "standard";
    /// <summary>The token filters to apply during lexical analysis.</summary>
    public string[] Filters { get; set; } = new string[0];
    /// <summary>The character filters to apply before tokenization.</summary>
    public string[] CharacterFilters { get; set; } = new string[0];

    /// <summary>Additional tokenizer arguments as a JSON string.</summary>
    public string TokenizerArgumentsJson { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="LexicalOptionsAttribute"/> with default settings.
    /// </summary>
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


