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


public class TableDefinition
{
  public TableDefinition()
  { }
  /*
"definition": {
"columns": {
 "title": {
   "type": "text"
 },
 "number_of_pages": {
   "type": "int"
 },
 "rating": {
   "type": "float"
 },
 "metadata": {
   "type": "map",
   "keyType": "text",
   "valueType": "text"
 },
 "genres": {
   "type": "set",
   "valueType": "text"
 },
 "is_checked_out": {
   "type": "boolean"
 },
 "due_date": {
   "type": "date"
 },
 "example_vector": {
   "type": "vector",
   "dimension": 1024
 },
 # This column will store vector embeddings.
 # The {embedding-provider-name} integration
 # will automatically generate vector embeddings
 # for any text inserted to this column.
 "example_vector": {
   "type": "vector",
   "dimension": MODEL_DIMENSIONS,
   "service": {
     "provider": "PROVIDER",
     "modelName": "MODEL_NAME",
     "authentication": {
       "providerKey": "API_KEY_NAME"
     },
     "parameters": PARAMETERS
   }
 },
 # If you want to store the original text
 # in addition to the generated embeddings
 # you must create a separate column.
 "original_text": "text"
},
"primaryKey": "title"
}
*/
  [JsonPropertyName("columns")]
  [JsonInclude]
  public Dictionary<string, Column> Columns { get; set; } = new Dictionary<string, Column>();

  [JsonPropertyName("primaryKey")]
  [JsonInclude]
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
  public PrimaryKeyDefinition PrimaryKey { get; set; }
  /*
  SINGLE COLUMN PRIMARY KEY
  "primaryKey": "title"
  COMPOSITE PRIMARY KEY
  "primaryKey": {
      "partitionBy": [
        "title", "rating"
      ]
    }
  COMPOUND PRIMARY KEY
  "primaryKey": {
      "partitionBy": [
        "title",
        "rating"
      ],
      "partitionSort": {
        "number_of_pages": 1,
        "is_checked_out": -1
      }
    }
  */

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
              //TODO: change to check for all numeric array types?
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
        definition.AddDateColumn(columnName);
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
        if (propertyType.IsArray)
        {
          Type elementType = propertyType.GetElementType();
          if (elementType == typeof(byte))
          {
            definition.AddBlobColumn(columnName);
          }
          //TODO: other array types
        }
        else if (propertyType.IsEnum)
        {
          //TODO (int??)
          break;
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
      // TypeCode.Object => "object",
      // TypeCode.Byte => throw new NotImplementedException(),
      // TypeCode.Char => throw new NotImplementedException(),
      // TypeCode.DBNull => throw new NotImplementedException(),
      // TypeCode.Double => throw new NotImplementedException(),
      // TypeCode.Empty => throw new NotImplementedException(),
      // TypeCode.Int16 => throw new NotImplementedException(),
      // TypeCode.Int64 => throw new NotImplementedException(),
      // TypeCode.SByte => throw new NotImplementedException(),
      // TypeCode.Single => throw new NotImplementedException(),
      // TypeCode.UInt16 => throw new NotImplementedException(),
      // TypeCode.UInt32 => throw new NotImplementedException(),
      // TypeCode.UInt64 => throw new NotImplementedException(),
      _ => "text",
    };
  }
  // return typeValue switch
  //     {
  //         "text" => JsonSerializer.Deserialize<TextColumn>(jsonText, options),
  //         "ascii" => JsonSerializer.Deserialize<TextColumn>(jsonText, options),
  //         "varchar" => JsonSerializer.Deserialize<TextColumn>(jsonText, options),
  //         "inet" => JsonSerializer.Deserialize<IPAddressColumn>(jsonText, options),
  //         "int" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
  //         "tinyint" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
  //         "smallint" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
  //         "varint" => JsonSerializer.Deserialize<IntColumn>(jsonText, options),
  //         "bigint" => JsonSerializer.Deserialize<LongColumn>(jsonText, options),
  //         "decimal" => JsonSerializer.Deserialize<DecimalColumn>(jsonText, options),
  //         "double" => JsonSerializer.Deserialize<DoubleColumn>(jsonText, options),
  //         "float" => JsonSerializer.Deserialize<FloatColumn>(jsonText, options),
  //         "map" => JsonSerializer.Deserialize<DictionaryColumn>(jsonText, options),
  //         "set" => JsonSerializer.Deserialize<SetColumn>(jsonText, options),
  //         "list" => JsonSerializer.Deserialize<ListColumn>(jsonText, options),
  //         "boolean" => JsonSerializer.Deserialize<BooleanColumn>(jsonText, options),
  //         "date" => JsonSerializer.Deserialize<DateColumn>(jsonText, options),
  //         "time" => JsonSerializer.Deserialize<DateColumn>(jsonText, options),
  //         "timestamp" => JsonSerializer.Deserialize<DateColumn>(jsonText, options),
  //         "vector" => JsonSerializer.Deserialize<VectorColumn>(jsonText, options),
  //         "uuid" => JsonSerializer.Deserialize<UUIDColumn>(jsonText, options),
  //         "blob" => JsonSerializer.Deserialize<BlobColumn>(jsonText, options),
  //         _ => throw new JsonException($"Unknown Column type '{typeValue}' encountered.")
  //     };

}

//TODO: standardize when we use enums with attributes specifying how to serialize, enums with tostring() extension methods, and constants.

internal class ColumnTypeConstants
{
  internal const string Text = "text";
}

public static class TableDefinitionExtensions
{

  public static TableDefinition AddSinglePrimaryKey(this TableDefinition tableDefinition, string keyName)
  {
    return tableDefinition.AddCompoundPrimaryKey(keyName, 1);
  }

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

  public static TableDefinition AddTextColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new TextColumn());
    return tableDefinition;
  }

  public static TableDefinition AddIntColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new IntColumn());
    return tableDefinition;
  }

  public static TableDefinition AddLongColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new LongColumn());
    return tableDefinition;
  }

  public static TableDefinition AddFloatColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new FloatColumn());
    return tableDefinition;
  }

  public static TableDefinition AddBooleanColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new BooleanColumn());
    return tableDefinition;
  }

  public static TableDefinition AddDateColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DateTimeColumn());
    return tableDefinition;
  }

  public static TableDefinition AddVectorColumn(this TableDefinition tableDefinition, string columnName, int dimension)
  {
    tableDefinition.Columns.Add(columnName, new VectorColumn(dimension));
    return tableDefinition;
  }

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

  public static TableDefinition AddVectorizeColumn(this TableDefinition tableDefinition, string columnName, int dimension, VectorServiceOptions options)
  {
    tableDefinition.Columns.Add(columnName, new VectorizeColumn(dimension, options));
    return tableDefinition;
  }

  public static TableDefinition AddDecimalColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DecimalColumn());
    return tableDefinition;
  }

  public static TableDefinition AddDoubleColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DoubleColumn());
    return tableDefinition;
  }

  public static TableDefinition AddUUIDColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new GuidColumn());
    return tableDefinition;
  }

  public static TableDefinition AddBlobColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new BlobColumn());
    return tableDefinition;
  }

  public static TableDefinition AddGuidColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new GuidColumn());
    return tableDefinition;
  }

  public static TableDefinition AddIPAddressColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new IPAddressColumn());
    return tableDefinition;
  }

  public static TableDefinition AddDurationColumn(this TableDefinition tableDefinition, string columnName)
  {
    tableDefinition.Columns.Add(columnName, new DurationColumn());
    return tableDefinition;
  }

  public static TableDefinition AddDictionaryColumn(this TableDefinition tableDefinition, string columnName, Type keyType, Type valueType)
  {
    tableDefinition.Columns.Add(columnName, new DictionaryColumn(TableDefinition.GetColumnTypeName(keyType), TableDefinition.GetColumnTypeName(valueType)));
    return tableDefinition;
  }

  public static TableDefinition AddSetColumn(this TableDefinition tableDefinition, string columnName, Type valueType)
  {
    tableDefinition.Columns.Add(columnName, new SetColumn(TableDefinition.GetColumnTypeName(valueType)));
    return tableDefinition;
  }

  public static TableDefinition AddListColumn(this TableDefinition tableDefinition, string columnName, Type valueType)
  {
    tableDefinition.Columns.Add(columnName, new ListColumn(TableDefinition.GetColumnTypeName(valueType)));
    return tableDefinition;
  }

}
