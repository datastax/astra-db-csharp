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

using System.Collections.Generic;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// A single layer of 'caller information' for self-identifying when issuing API requests.
/// </summary>
public class APICaller
{
    /// <summary>
    /// Name of the caller (in most cases, a package name)
    /// </summary>
    public string Name { get; set; } = null;

    /// <summary>
    /// Version of the caller, typically in dot-notation.
    /// </summary>
    public string Version { get; set; } = null;

    internal string ToString()
    {
        if (Name == null && Version == null)
        {
            return null;
        }
        var parts = new List<string>();
        if (Name != null) parts.Add(Name);
        if (Version != null) parts.Add(Version);
        return string.Join("/", parts);
    }

    static internal string ToString(List<APICaller> callers) {
        var callerStrings = new List<string>();
        foreach (var caller in callers)
        {
            var callerString = caller.ToString();
            if (callerString != null) callerStrings.Add(callerString);
        }
        return string.Join(" ", callerStrings);
    }
}
