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
/// Marks a column as a vector column with a specified number of dimensions.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class ColumnVectorAttribute : Attribute
{
    /// <summary>
    /// The number of dimensions for the vector column.
    /// </summary>
    public int Dimension { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="ColumnVectorAttribute"/> with the specified dimension.
    /// </summary>
    /// <param name="dimension">The number of dimensions for the vector column.</param>
    public ColumnVectorAttribute(int dimension)
    {
        Dimension = dimension;
    }
}

