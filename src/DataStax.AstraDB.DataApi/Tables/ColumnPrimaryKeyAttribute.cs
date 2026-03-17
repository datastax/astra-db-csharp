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

using DataStax.AstraDB.DataApi.Core;
using System;

namespace DataStax.AstraDB.DataApi.Tables;


/// <summary>
/// Marks a property or field as part of the table's partition key.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class ColumnPrimaryKeyAttribute : Attribute
{
    /// <summary>
    /// The order of this column within the partition key (1-based).
    /// </summary>
    public int Order { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="ColumnPrimaryKeyAttribute"/> with a default order of 1.
    /// </summary>
    public ColumnPrimaryKeyAttribute()
    {
        Order = 1;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="ColumnPrimaryKeyAttribute"/> with the specified order.
    /// </summary>
    /// <param name="order">The order of this column within the partition key (1-based).</param>
    public ColumnPrimaryKeyAttribute(int order)
    {
        Order = order;
    }
}

// [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
// public class ColumnSinglePrimaryKeyAttribute : ColumnCompoundPrimaryKeyAttribute
// {
//     public ColumnSinglePrimaryKeyAttribute() : base(0) { }
// }

/// <summary>
/// Marks a property or field as a clustering key in the table's primary key, with a specified sort order.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public class ColumnPrimaryKeySortAttribute : Attribute
{
    /// <summary>
    /// The order of this column within the clustering key (1-based).
    /// </summary>
    public int Order { get; set; }

    /// <summary>
    /// The sort direction for this clustering key column.
    /// </summary>
    public SortDirection Direction { get; set; }

    /// <summary>
    /// Initializes a new instance of <see cref="ColumnPrimaryKeySortAttribute"/> with the specified order and sort direction.
    /// </summary>
    /// <param name="order">The order of this column within the clustering key (1-based).</param>
    /// <param name="direction">The sort direction for this clustering key column.</param>
    public ColumnPrimaryKeySortAttribute(int order, SortDirection direction)
    {
        Order = order;
        Direction = direction;
    }
}

