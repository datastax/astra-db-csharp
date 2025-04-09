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
    Before,
    After
}

public static class ReturnDocumentDirectiveExtensions
{
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