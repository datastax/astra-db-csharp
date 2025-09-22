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
using System.Net;
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
  public Dictionary<string, Column> Columns { get; set; } = new Dictionary<string, Column>();

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
        columnName = columnNameAttribute.ColumnName;
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
            definition.AddTextColumn(columnName);
            break;

          case ColumnVectorizeAttribute vectorize:
            createColumn = false;
            definition.AddVectorizeColumn(
                            columnName,
                            vectorize.Dimension,
                            vectorize.ServiceProvider,
                            vectorize.ServiceModelName,
                            vectorize.AuthenticationPairs?.ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1]),
                            vectorize.ParameterPairs?.ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1])
                        );
            break;

          case ColumnVectorAttribute vector:
            if (columnType != typeof(float[]) && columnType != typeof(double[]) && columnType != typeof(string))
            {
              throw new InvalidOperationException($"Vector Column {columnName} must be either float[], double[] or string (if sending already binary-encoded string)");
            }
            createColumn = false;
            definition.AddVectorColumn(columnName, vector.Dimension);
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
    switch (Type.GetTypeCode(propertyType))
    {
      case TypeCode.Int32:
      case TypeCode.Int16:
      case TypeCode.Byte:
        definition.AddIntColumn(columnName);
        break;
      case TypeCode.String:
        definition.AddTextColumn(columnName);
        break;
      case TypeCode.Boolean:
        definition.AddBooleanColumn(columnName);
        break;
      case TypeCode.DateTime:
        definition.AddDateTimeColumn(columnName);
        break;
      case TypeCode.Decimal:
        definition.AddDecimalColumn(columnName);
        break;
      case TypeCode.Double:
        definition.AddDoubleColumn(columnName);
        break;
      case TypeCode.Int64:
        definition.AddLongColumn(columnName);
        break;
      case TypeCode.Single:
        definition.AddFloatColumn(columnName);
        break;
      case TypeCode.Object:
        if (propertyType.FullName == "System.DateOnly")
        {
          definition.AddDateColumn(columnName);
        }
        else if (propertyType.FullName == "System.TimeOnly")
        {
          definition.AddTimeColumn(columnName);
        }
        else if (propertyType.IsArray)
        {
          Type elementType = propertyType.GetElementType();
          if (elementType == typeof(byte))
          {
            definition.AddBlobColumn(columnName);
          }
          else
          {
            definition.AddListColumn(columnName, elementType);
          }
        }
        else if (propertyType.IsEnum)
        {
          throw new NotSupportedException($"Enum types are not currently supported for column: {columnName}. Consider using a string or int property instead.");
        }
        else if (propertyType == typeof(Guid))
        {
          definition.AddGuidColumn(columnName);
        }
        else if (propertyType == typeof(Duration))
        {
          definition.AddDurationColumn(columnName);
        }
        else if (propertyType == typeof(IPAddress))
        {
          definition.AddIPAddressColumn(columnName);
        }
        else if (propertyType.IsGenericType)
        {
          Type genericTypeDefinition = propertyType.GetGenericTypeDefinition();
          Type[] genericArguments = propertyType.GetGenericArguments();

          if (genericTypeDefinition == typeof(Dictionary<,>))
          {
            if (genericArguments.Length == 2 && genericArguments[0] == typeof(string))
            {
              definition.AddDictionaryColumn(columnName, genericArguments[0], genericArguments[1]);
            }
            else
            {
              Console.WriteLine($"Warning: Unhandled Dictionary type for column: {columnName}. Only string keys are supported.");
            }
          }
          else if (genericTypeDefinition == typeof(List<>))
          {
            definition.AddListColumn(columnName, genericArguments[0]);
          }
          else if (genericTypeDefinition == typeof(HashSet<>))
          {
            definition.AddSetColumn(columnName, genericArguments[0]);
          }
          else
          {
            Console.WriteLine($"Warning: Unhandled generic type: {propertyType.Name} for column: {columnName}");
          }
        }
        else
        {
          Console.WriteLine($"Warning: Unhandled type: {propertyType.Name} for column: {columnName}");
        }
        break;
      default:
        Console.WriteLine($"Warning: Unhandled type code: {Type.GetTypeCode(propertyType)} for column: {columnName}");
        break;
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

  internal static string GetColumnTypeName(Type type)
  {
    return Type.GetTypeCode(type) switch
    {
      TypeCode.Int32 => "int",
      TypeCode.String => "text",
      TypeCode.Boolean => "boolean",
      TypeCode.DateTime => "date",
      TypeCode.Decimal => "decimal",
      _ => "text",
    };
  }

}

internal class ColumnTypeConstants
{
  internal const string Text = "text";
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

  /// <summary>
  /// Add a text column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddTextColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new TextColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add an int column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddIntColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new IntColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a long column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddLongColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new LongColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a float column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddFloatColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new FloatColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a boolean column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddBooleanColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new BooleanColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a date/time (timestamp) column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddDateTimeColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DateTimeColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a date column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddDateColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DateColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a time column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddTimeColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new TimeColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a vector column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <param name="dimension"></param>
  /// <returns></returns>
  public static TableDefinition AddVectorColumn(this TableDefinition tableDefinition, string columnName, int dimension)
  {
    tableDefinition.Columns.Add(columnName, new VectorColumn(dimension));
    return tableDefinition;
  }

  /// <summary>
  /// Add a vectorize column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <param name="dimension"></param>
  /// <param name="provider"></param>
  /// <param name="modelName"></param>
  /// <param name="authentication"></param>
  /// <param name="parameters"></param>
  /// <returns></returns>
  public static TableDefinition AddVectorizeColumn(this TableDefinition tableDefinition, string columnName, int dimension, string provider, string modelName, Dictionary<string, string> authentication, Dictionary<string, string> parameters)
  {
    tableDefinition.Columns.Add(columnName, new VectorizeColumn(dimension, new VectorServiceOptions
    {
      Provider = provider,
      ModelName = modelName,
      Authentication = authentication,
      Parameters = parameters
    }
    ));
    return tableDefinition;
  }

  /// <summary>
  /// Add a vectorize column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <param name="dimension"></param>
  /// <param name="options"></param>
  /// <returns></returns>
  public static TableDefinition AddVectorizeColumn(this TableDefinition tableDefinition, string columnName, int dimension, VectorServiceOptions options)
  {
    tableDefinition.Columns.Add(columnName, new VectorizeColumn(dimension, options));
    return tableDefinition;
  }

  /// <summary>
  /// Add a vectorize column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <param name="options"></param>
  /// <returns></returns>
  public static TableDefinition AddVectorizeColumn(this TableDefinition tableDefinition, string columnName, VectorServiceOptions options)
  {
    tableDefinition.Columns.Add(columnName, new VectorizeColumn(options));
    return tableDefinition;
  }

  /// <summary>
  /// Add a decimal column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddDecimalColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DecimalColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a double column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddDoubleColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DoubleColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a UUID column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddUUIDColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new GuidColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a blob column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddBlobColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new BlobColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a GUID column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddGuidColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new GuidColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add an IP Address column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddIPAddressColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new IPAddressColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a Duration column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <returns></returns>
  public static TableDefinition AddDurationColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DurationColumn());
    return tableDefinition;
  }

  /// <summary>
  /// Add a dictionary column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <param name="keyType"></param>
  /// <param name="valueType"></param>
  /// <returns></returns>
  public static TableDefinition AddDictionaryColumn(this TableDefinition tableDefinition, string columnName, Type keyType, Type valueType)
  {
    tableDefinition.Columns.Add(columnName, new DictionaryColumn(TableDefinition.GetColumnTypeName(keyType), TableDefinition.GetColumnTypeName(valueType)));
    return tableDefinition;
  }

  /// <summary>
  /// Add a set column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <param name="valueType"></param>
  /// <returns></returns>
  public static TableDefinition AddSetColumn(this TableDefinition tableDefinition, string columnName, Type valueType)
  {
    tableDefinition.Columns.Add(columnName, new SetColumn(TableDefinition.GetColumnTypeName(valueType)));
    return tableDefinition;
  }

  /// <summary>
  /// Add a list column to the table definition
  /// </summary>
  /// <param name="tableDefinition"></param>
  /// <param name="columnName"></param>
  /// <param name="valueType"></param>
  /// <returns></returns>
  public static TableDefinition AddListColumn(this TableDefinition tableDefinition, string columnName, Type valueType)
  {
    tableDefinition.Columns.Add(columnName, new ListColumn(TableDefinition.GetColumnTypeName(valueType)));
    return tableDefinition;
  }

}
