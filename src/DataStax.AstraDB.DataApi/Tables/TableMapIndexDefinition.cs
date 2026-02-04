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

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Specifies which piece of a map to index for a table column.
/// </summary>
public enum MapIndexType
{
  /// <summary>
  /// Index the keys of the map.
  /// </summary>
  Keys,
  /// <summary>
  /// Index the values of the map.
  /// </summary>
  Values,
  /// <summary>
  /// Index the entries of the map.
  /// </summary>
  Entries
}

/// <summary>
/// Configuration used to create a text index on a table column
/// </summary>
public class TableMapIndexDefinition : TableIndexDefinition
{

  internal TableMapIndexDefinition(MapIndexType indexType)
  {
    IndexType = indexType;
  }

  internal override object Column
  {
    get
    {
      if (IndexType == MapIndexType.Entries)
      {
        return ColumnName;
      }
      string indexTypeToken = IndexType switch
      {
        MapIndexType.Values => "$values",
        MapIndexType.Keys => "$keys",
        _ => throw new System.InvalidOperationException("Invalid MapIndexType")
      };
      return new Dictionary<string, string>
      {
        [ColumnName] = indexTypeToken
      };
    }
  }

  internal MapIndexType IndexType { get; set; }

}
