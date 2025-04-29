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

using DataStax.AstraDB.DataApi.Core;
using System;

/// <summary>
/// Marks a property on a document as the unique ID
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class DocumentIdAttribute : Attribute
{
    /// <summary>
    /// If the database is going to auto-generate ids for inserted document, which type of ID should it generate?
    /// </summary>
    public DefaultIdType? DefaultIdType { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="DocumentIdAttribute"/> class.
    /// </summary>
    /// <remarks>
    /// 1. If you want the database to auto-generate Ids for the documents, please use the <see cref="DocumentIdAttribute(DefaultIdType)"/> overload.
    /// When not specified, DefaultIdType.Uuid (UUID v4) will be used.
    /// 2. Please note that the C# type used for the property/field that this attribute is assigned to must match the type associated
    /// with the specified default ID (or you need to always pass in a value for the Id field).
    /// </remarks>
    public DocumentIdAttribute()
    {

    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DocumentIdAttribute"/> class.
    /// </summary>
    /// <param name="defaultIdType">The type of Id to auto-generate</param>
    /// <remarks>
    /// The C# type used for the property/field that this attribute is assigned to must match the type associated
    /// with the specified default ID (or use the <see cref="DocumentIdAttribute()"/> constructor to not set a default type 
    /// and always pass in a value for the Id field.
    /// </remarks>
    public DocumentIdAttribute(DefaultIdType defaultIdType)
    {
        DefaultIdType = defaultIdType;
    }
}