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
using System.Text.Json.Serialization;
using DataStax.AstraDB.DataApi.Core;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Represents an alter table operation that can be converted to a JSON fragment for transmission.
/// </summary>
public interface IAlterTableOperation
{
  /// <summary>
  /// Converts the operation to its JSON representation.
  /// </summary>
  /// <returns>A serializable object representing the operation.</returns>
  object ToJsonFragment();
}

/// <summary>
/// Represents an operation to add new columns to a table.
/// </summary>
public class AlterTableAddColumns : IAlterTableOperation
{
  /// <summary>
  /// Gets the columns to be added.
  /// </summary>
  public Dictionary<string, AlterTableColumnDefinition> Columns { get; }

  /// <summary>
  /// Initializes a new instance with the specified columns.
  /// </summary>
  /// <param name="columns">The columns to add.</param>
  public AlterTableAddColumns(Dictionary<string, AlterTableColumnDefinition> columns)
  {
    Columns = columns;
  }

  /// <inheritdoc/>
  public object ToJsonFragment() => new
  {
    add = new
    {
      columns = Columns
    }
  };
}

/// <summary>
/// Describes the definition of a single column in an alter operation.
/// </summary>
public class AlterTableColumnDefinition
{
  /// <summary>
  /// Gets or sets the type of the column.
  /// </summary>
  [JsonPropertyName("type")]
  public string Type { get; set; }

  /// <summary>
  /// Gets or sets the key type if the column is a map or similar structure.
  /// </summary>
  [JsonPropertyName("keyType")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public string KeyType { get; set; }

  /// <summary>
  /// Gets or sets the value type if the column is a map or similar structure.
  /// </summary>
  [JsonPropertyName("valueType")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public string ValueType { get; set; }
}

/// <summary>
/// Represents an operation to add vector columns to a table.
/// </summary>
public class AlterTableAddVectorColumns : IAlterTableOperation
{
  /// <summary>
  /// Gets the vector columns to be added.
  /// </summary>
  public Dictionary<string, AlterTableVectorColumnDefinition> Columns { get; }

  /// <summary>
  /// Initializes a new instance with the specified vector columns.
  /// </summary>
  /// <param name="columns">The vector columns to add.</param>
  public AlterTableAddVectorColumns(Dictionary<string, AlterTableVectorColumnDefinition> columns)
  {
    Columns = columns;
  }

  /// <inheritdoc/>
  public object ToJsonFragment() => new
  {
    add = new
    {
      columns = Columns
    }
  };
}

/// <summary>
/// Describes the definition of a vector column.
/// </summary>
public class AlterTableVectorColumnDefinition
{
  /// <summary>
  /// Gets the type of the column. Always returns "vector".
  /// </summary>
  [JsonPropertyName("type")]
  public string Type => "vector";

  /// <summary>
  /// Gets or sets the vector dimension.
  /// </summary>
  [JsonPropertyName("dimension")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public int? VectorDimension { get; set; }

  /// <summary>
  /// Gets or sets the vector service options.
  /// </summary>
  [JsonPropertyName("service")]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public VectorServiceOptions Service { get; set; }
}

/// <summary>
/// Represents an operation to drop columns from a table.
/// </summary>
public class AlterTableDropColumns : IAlterTableOperation
{
  /// <summary>
  /// Gets the list of column names to drop.
  /// </summary>
  public List<string> Columns { get; }

  /// <summary>
  /// Initializes a new instance with the specified column names.
  /// </summary>
  /// <param name="columns">The column names to drop.</param>
  public AlterTableDropColumns(IEnumerable<string> columns)
  {
    Columns = new List<string>(columns);
  }

  /// <inheritdoc/>
  public object ToJsonFragment() => new
  {
    drop = new
    {
      columns = Columns
    }
  };
}

/// <summary>
/// Represents an operation to add vectorization services to specific columns.
/// </summary>
public class AlterTableAddVectorize : IAlterTableOperation
{
  /// <summary>
  /// Gets the columns and associated vector service options.
  /// </summary>
  public Dictionary<string, VectorServiceOptions> Columns { get; }

  /// <summary>
  /// Initializes a new instance with the specified vectorization settings.
  /// </summary>
  /// <param name="columns">The columns and their vector services.</param>
  public AlterTableAddVectorize(Dictionary<string, VectorServiceOptions> columns)
  {
    Columns = columns;
  }

  /// <inheritdoc/>
  public object ToJsonFragment() => new
  {
    addVectorize = new
    {
      columns = Columns
    }
  };
}

/// <summary>
/// Represents an operation to remove vectorization from specific columns.
/// </summary>
public class AlterTableDropVectorize : IAlterTableOperation
{
  /// <summary>
  /// Gets the list of column names to remove vectorization from.
  /// </summary>
  public List<string> Columns { get; }

  /// <summary>
  /// Initializes a new instance with the specified column names.
  /// </summary>
  /// <param name="columns">The columns to remove vectorization from.</param>
  public AlterTableDropVectorize(IEnumerable<string> columns)
  {
    Columns = new List<string>(columns);
  }

  /// <inheritdoc/>
  public object ToJsonFragment() => new
  {
    dropVectorize = new
    {
      columns = Columns
    }
  };
}
