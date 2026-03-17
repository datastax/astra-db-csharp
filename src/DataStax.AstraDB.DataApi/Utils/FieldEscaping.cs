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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataStax.AstraDB.DataApi.Utils;

/// <summary>
/// Utility for escaping and unescaping field names for use in Data API field paths.
/// Dots and ampersands within individual field name segments are escaped so they are
/// not interpreted as path separators or escape characters.
/// </summary>
public static class FieldEscaping
{
    private static readonly Regex _escapeRegex = new("([.&])", RegexOptions.Compiled);

    /// <summary>
    /// Escapes each segment and joins them with '.' to produce a valid Data API field path.
    /// </summary>
    public static string EscapeFieldNames(params string[] segments)
    {
        return EscapeFieldNames((IEnumerable<string>)segments);
    }

    /// <summary>
    /// Escapes each segment and joins them with '.' to produce a valid Data API field path.
    /// </summary>
    public static string EscapeFieldNames(IEnumerable<string> segments)
    {
        if (segments == null)
        {
            return "";
        }
        
        return string.Join(".", segments.Select(s => 
            _escapeRegex.Replace(s, "&$1")
        ));
    }

    /// <summary>
    /// Splits an escaped Data API field path back into its individual unescaped segments.
    /// </summary>
    public static string[] UnescapeFieldPath(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            return Array.Empty<string>();
        }

        if (!path.Contains('&') && !path.Contains('.'))
        {
            return new[] { path };
        }

        if (path.StartsWith("."))
        {
            throw new ArgumentException($"Invalid field path '{path}'; '.' may not appear at the beginning of the path", nameof(path));
        }

        var ret = new List<string>();
        var segment = new StringBuilder();
        var length = path.Length;

        for (int i = 0; i <= length; i++)
        {
            if (i < length && path[i] == '.' && i == length - 1)
            {
                throw new ArgumentException($"Invalid field path '{path}'; '.' may not appear at the end of the path", nameof(path));
            }
            
            if (i == length || path[i] == '.')
            {
                if (segment.Length == 0)
                {
                    throw new ArgumentException($"Invalid field path '{path}'; empty segment found at position {i}", nameof(path));
                }
            
                ret.Add(segment.ToString());
                segment.Clear();
            }
            else if (path[i] == '&')
            {
                if (i + 1 == length)
                {
                    throw new ArgumentException($"Invalid escape sequence in field path '{path}'; '&' may not appear at the end of the path", nameof(path));
                }

                var c = path[++i];

                if (c == '&' || c == '.')
                {
                    segment.Append(c);
                }
                else
                {
                    throw new ArgumentException($"Invalid escape sequence in field path '{path}' at position {i - 1}; '&' may not appear alone (must be used as either '&&' or '&.')", nameof(path));
                }
            }
            else
            {
                segment.Append(path[i]);
            }
        }

        return ret.ToArray();
    }
}