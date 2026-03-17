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
/// Whether to return the document before or after the replacement.
/// </summary>
public enum ReturnDocumentDirective
{
    /// <summary>Returns the document as it was before the operation was applied.</summary>
    Before,
    /// <summary>Returns the document as it is after the operation was applied.</summary>
    After
}

/// <summary>
/// Extension methods for <see cref="ReturnDocumentDirective"/>.
/// </summary>
public static class ReturnDocumentDirectiveExtensions
{
    /// <summary>
    /// Serializes a <see cref="ReturnDocumentDirective"/> value to its Data API string representation.
    /// </summary>
    /// <param name="returnDocumentDirective">The directive to serialize.</param>
    /// <returns>"before", "after", or <c>null</c> if the value is <c>null</c>.</returns>
    public static string Serialize(this ReturnDocumentDirective? returnDocumentDirective)
    {
        if (returnDocumentDirective == null) return null;
        return returnDocumentDirective switch
        {
            ReturnDocumentDirective.Before => "before",
            ReturnDocumentDirective.After => "after",
            _ => throw new ArgumentOutOfRangeException(),
        };
    }
}