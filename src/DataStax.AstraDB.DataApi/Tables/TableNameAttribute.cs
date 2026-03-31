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
/// Overrides the table name used when mapping a class to a database table.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class TableNameAttribute : Attribute
{
    /// <summary>
    /// The table name to use in the database.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="TableNameAttribute"/> with the specified table name.
    /// </summary>
    /// <param name="name">The table name to use in the database.</param>
    public TableNameAttribute(string name)
    {
        Name = name;
    }
}