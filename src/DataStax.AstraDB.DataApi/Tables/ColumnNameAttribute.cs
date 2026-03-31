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

/// <summary>
/// Overrides the column name used when mapping a property or field to a table column.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class ColumnNameAttribute : Attribute
{
    /// <summary>
    /// The column name to use in the table.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="ColumnNameAttribute"/> with the specified column name.
    /// </summary>
    /// <param name="columnName">The column name to use in the table.</param>
    public ColumnNameAttribute(string columnName)
    {
        Name = columnName;
    }
}