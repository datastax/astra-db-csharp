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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;

/// <summary>
/// Definition for creating a table
/// </summary>
public class TableDefinition
{
  /// <summary>
  /// The columns for this table
  /// </summary>
  [JsonPropertyName("columns")]
  [JsonInclude]
  internal Dictionary<string, object> Columns { get; set; } = new Dictionary<string, object>();

  /// <summary>
  /// The primary key definition for this table
  /// </summary>
  [JsonPropertyName("primaryKey")]
  [JsonInclude]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public PrimaryKeyDefinition PrimaryKey { get; set; }

  internal static TableDefinition CreateTableDefinition<T>() where T : new()
  {
    var definition = new TableDefinition();

    Type type = typeof(T);
    PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

    foreach (PropertyInfo property in properties)
    {
      var attributes = property.GetCustomAttributes(true);

      if (attributes.Any(attr => attr is ColumnIgnoreAttribute))
      {
        continue;
      }

      var createColumn = true;

      var propertyType = property.PropertyType;
      Type underlyingType;
      if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
      {
        underlyingType = Nullable.GetUnderlyingType(propertyType);
      }
      else
      {
        underlyingType = propertyType;
      }

      var columnType = underlyingType ?? propertyType;

      string columnName = property.Name;
      var columnNameAttribute = attributes.OfType<ColumnNameAttribute>().FirstOrDefault();
      if (columnNameAttribute != null)
      {
        columnName = columnNameAttribute.Name;
      }

      foreach (var attribute in attributes)
      {
        switch (attribute)
        {
          case ColumnPrimaryKeyAttribute primaryKey:
            definition.AddCompoundPrimaryKey(columnName, primaryKey.Order);
            break;

          case ColumnPrimaryKeySortAttribute primaryKeySort:
            definition.AddCompoundPrimaryKeySort(columnName, primaryKeySort.Order, primaryKeySort.Direction);
            break;

          case ColumnJsonStringAttribute json:
            createColumn = false;
            definition.AddColumn(columnName, DataApiType.Text());
            break;

          case ColumnVectorizeAttribute vectorize:
            createColumn = false;
            definition.AddColumn(columnName, new VectorizeDataApiType(vectorize.Dimension, new VectorServiceOptions()
            {
              Provider = vectorize.ServiceProvider,
              ModelName = vectorize.ServiceModelName,
              Authentication = vectorize.AuthenticationPairs?.ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1]),
              Parameters = vectorize.ParameterPairs?.ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1])
            }));
            // definition.AddVectorizeColumn(
            //                 columnName,
            //                 vectorize.Dimension,
            //                 vectorize.ServiceProvider,
            //                 vectorize.ServiceModelName,
            //                 vectorize.AuthenticationPairs?.ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1]),
            //                 vectorize.ParameterPairs?.ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1])
            //             );
            break;

          case ColumnVectorAttribute vector:
            if (columnType != typeof(float[]) && columnType != typeof(double[]) && columnType != typeof(string))
            {
              throw new InvalidOperationException($"Vector Column {columnName} must be either float[], double[] or string (if sending already binary-encoded string)");
            }
            createColumn = false;
            definition.AddColumn(columnName, DataApiType.Vector(vector.Dimension));
            //definition.AddVectorColumn(columnName, vector.Dimension);
            break;
        }
      }

      if (createColumn)
      {
        CreateColumnFromPropertyType(columnName, columnType, definition);
      }
    }

    if (definition.PrimaryKey == null)
    {
      throw new InvalidOperationException("No primary key defined for table class. Please use a ColumnPrimaryKeyAttribute (for a single column primary key) or multiple ColumnPrimaryKeyAttributes (for compound primary keys or with ColumnPrimaryKeySortAttribute for composite primary keys).");
    }

    return definition;
  }

  private static void CreateColumnFromPropertyType(string columnName, Type propertyType, TableDefinition definition)
  {
    var type = TypeUtilities.GetDataApiTypeFromUnderlyingType(propertyType);
    if (type != null)
    {
      definition.AddColumn(columnName, type);
    }
  }

  internal static string GetTableName<TRow>() where TRow : class, new()
  {
    Type type = typeof(TRow);
    var tableNameAttribute = type.GetCustomAttribute<TableNameAttribute>();
    if (tableNameAttribute != null)
    {
      return tableNameAttribute.Name;
    }
    return type.Name;
  }

}

/// <summary>
/// Extension methods for building table definitions
/// </summary>
public static class TableDefinitionExtensions
{

  /// <summary>
  /// Add a single column primary key to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="keyName"></param>
  /// <returns></returns>
  public static TableDefinition AddSinglePrimaryKey(this TableDefinition tableDefinition, string keyName)
  {
    return tableDefinition.AddCompoundPrimaryKey(keyName, 1);
  }

  /// <summary>
  /// Add a composite primary key to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="keyNames"></param>
  /// <returns></returns>
  /// <exception cref="ArgumentException"></exception>
  public static TableDefinition AddCompositePrimaryKey(this TableDefinition tableDefinition, string[] keyNames)
  {
    if (keyNames == null || keyNames.Length == 0)
    {
      throw new ArgumentException("Key names cannot be null or empty.", nameof(keyNames));
    }

    var primaryKey = tableDefinition.PrimaryKey ?? new PrimaryKeyDefinition();
    for (int i = 0; i < keyNames.Length; i++)
    {
      primaryKey.KeyList.Add(i + 1, keyNames[i]);
    }
    tableDefinition.PrimaryKey = primaryKey;
    return tableDefinition;
  }

  /// <summary>
  /// Add a compound primary key to the table definition with partition key sorts
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="keyNames"></param>
  /// <param name="partitionSorts"></param>
  /// <returns></returns>
  /// <exception cref="ArgumentException"></exception>
  public static TableDefinition AddCompoundPrimaryKey(this TableDefinition tableDefinition, string[] keyNames, PrimaryKeySort[] partitionSorts)
  {
    if (keyNames == null || keyNames.Length == 0)
    {
      throw new ArgumentException("Key names cannot be null or empty.", nameof(keyNames));
    }

    var primaryKey = tableDefinition.PrimaryKey ?? new PrimaryKeyDefinition();
    for (int i = 0; i < keyNames.Length; i++)
    {
      primaryKey.KeyList.Add(i + 1, keyNames[i]);
    }
    for (int i = 0; i < partitionSorts.Length; i++)
    {
      primaryKey.SortList.Add(i + 1, partitionSorts[i]);
    }
    tableDefinition.PrimaryKey = primaryKey;
    return tableDefinition;
  }

  internal static TableDefinition AddCompoundPrimaryKey(this TableDefinition tableDefinition, string keyName, int order)
  {
    var primaryKey = tableDefinition.PrimaryKey ?? new PrimaryKeyDefinition();
    primaryKey.KeyList.Add(order, keyName);
    tableDefinition.PrimaryKey = primaryKey;
    return tableDefinition;
  }

  internal static TableDefinition AddCompoundPrimaryKeySort(this TableDefinition tableDefinition, string keyName, int keyOrder, SortDirection direction)
  {
    var primaryKey = tableDefinition.PrimaryKey ?? new PrimaryKeyDefinition();
    primaryKey.SortList.Add(keyOrder, new PrimaryKeySort(keyName, direction));
    tableDefinition.PrimaryKey = primaryKey;
    return tableDefinition;
  }

  public static TableDefinition AddColumn(this TableDefinition tableDefinition, string columnName, DataApiType columnType)
  {
    tableDefinition.Columns.Add(columnName, columnType.AsColumnType);
    return tableDefinition;
  }

  // /// <summary>
  // /// Add a text column to the table definition
  // /// </summary>
  // /// <param name="tableDefinition"></param>
  // /// <param name="columnName"></param>
  // /// <param name="columnType"></param>
  // /// <param name="keyType">(Optional) Type of the keys for this column (e.g. for map columns)</param>
  // /// <param name="valueType">(Optional) Type of the values to be stored in this column (e.g. for list types)</param>
  // /// <returns></returns>
  // public static TableDefinition AddColumn(this TableDefinition tableDefinition, string columnName, DataApiType columnType, DataApiType keyType = default, DataApiType valueType = default)
  // {
  //   tableDefinition.Columns.Add(columnName, new Column() { Type = columnType, KeyType = keyType, ValueType = valueType });
  //   return tableDefinition;
  // }

  // /// <summary>
  // /// Add a vector column to the table definition
  // /// </summary>
  // /// <param name="tableDefinition"></param>
  // /// <param name="columnName"></param>
  // /// <param name="dimension"></param>
  // /// <returns></returns>
  // public static TableDefinition AddVectorColumn(this TableDefinition tableDefinition, string columnName, int dimension)
  // {
  //   tableDefinition.Columns.Add(columnName, new VectorColumn(dimension));
  //   return tableDefinition;
  // }

  // /// <summary>
  // /// Add a vectorize column to the table definition
  // /// </summary>
  // /// <param name="tableDefinition"></param>
  // /// <param name="columnName"></param>
  // /// <param name="dimension"></param>
  // /// <param name="provider"></param>
  // /// <param name="modelName"></param>
  // /// <param name="authentication"></param>
  // /// <param name="parameters"></param>
  // /// <returns></returns>
  // public static TableDefinition AddVectorizeColumn(this TableDefinition tableDefinition, string columnName, int dimension, string provider, string modelName, Dictionary<string, string> authentication, Dictionary<string, string> parameters)
  // {
  //   tableDefinition.Columns.Add(columnName, new VectorizeColumn(dimension, new VectorServiceOptions
  //   {
  //     Provider = provider,
  //     ModelName = modelName,
  //     Authentication = authentication,
  //     Parameters = parameters
  //   }
  //   ));
  //   return tableDefinition;
  // }

  // /// <summary>
  // /// Add a vectorize column to the table definition
  // /// </summary>
  // /// <param name="tableDefinition"></param>
  // /// <param name="columnName"></param>
  // /// <param name="dimension"></param>
  // /// <param name="options"></param>
  // /// <returns></returns>
  // public static TableDefinition AddVectorizeColumn(this TableDefinition tableDefinition, string columnName, int dimension, VectorServiceOptions options)
  // {
  //   tableDefinition.Columns.Add(columnName, new VectorizeColumn(dimension, options));
  //   return tableDefinition;
  // }

  // /// <summary>
  // /// Add a vectorize column to the table definition
  // /// </summary>
  // /// <param name="tableDefinition"></param>
  // /// <param name="columnName"></param>
  // /// <param name="options"></param>
  // /// <returns></returns>
  // public static TableDefinition AddVectorizeColumn(this TableDefinition tableDefinition, string columnName, VectorServiceOptions options)
  // {
  //   tableDefinition.Columns.Add(columnName, new VectorizeColumn(options));
  //   return tableDefinition;
  // }

}
