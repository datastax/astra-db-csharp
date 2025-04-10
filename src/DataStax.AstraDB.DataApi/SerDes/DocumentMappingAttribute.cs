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

namespace DataStax.AstraDB.DataApi.SerDes;

using System;

/// <summary>
/// Marks a property on a document as a special field for the database's use.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class DocumentMappingAttribute : Attribute
{
    /// <summary>
    /// The type of special field this property is mapped to
    /// </summary>
    public DocumentMappingField Field { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="DocumentMappingAttribute"/> class.
    /// </summary>
    /// <param name="field">The type of special field this property is mapped to</param>
    public DocumentMappingAttribute(DocumentMappingField field)
    {
        Field = field;
    }
}