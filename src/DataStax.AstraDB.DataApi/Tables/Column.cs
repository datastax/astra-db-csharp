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
using DataStax.AstraDB.DataApi.SerDes;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Tables;


/// <summary>
/// Represents a column in a table
/// </summary>
[JsonConverter(typeof(ColumnConverter))]
public abstract class Column
{
    /// <summary>
    /// The type of the column
    /// </summary>
    [JsonIgnore]
    public abstract string Type { get; }
}

/// <summary>
/// A column that holds text values
/// </summary>
public class TextColumn : Column
{
    /// <summary>
    /// Defines the column as type text
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "text";
}

/// <summary>
/// A column that holds UUID values
/// </summary>
public class GuidColumn : Column
{
    /// <summary>
    /// Defines the column as type uuid
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "uuid";
}

/// <summary>
/// A column that holds integer values
/// </summary>
public class IntColumn : Column
{
    /// <summary>
    /// Defines the column as type int
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "int";
}

/// <summary>
/// A column that holds long integer values
/// </summary>
public class LongColumn : Column
{
    /// <summary>
    /// Defines the column as type bigint
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "bigint";
}

/// <summary>
/// A column that holds decimal values
/// </summary>
public class DecimalColumn : Column
{
    /// <summary>
    /// Defines the column as type decimal
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "decimal";
}

/// <summary>
/// A column that holds float values
/// </summary>
public class FloatColumn : Column
{
    /// <summary>
    /// Defines the column as type float
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "float";
}

/// <summary>
/// A column that holds double values
/// </summary>
public class DoubleColumn : Column
{
    /// <summary>
    /// Defines the column as type double
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "double";
}

/// <summary>
/// A column that holds boolean values
/// </summary>
public class BooleanColumn : Column
{
    /// <summary>
    /// Defines the column as type boolean
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "boolean";
}

/// <summary>
/// A column that holds date/time values
/// </summary>
public class DateTimeColumn : Column
{
    /// <summary>
    /// Defines the column as type timestamp
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "timestamp";
}

/// <summary>
/// A column that holds binary values
/// </summary>
public class BlobColumn : Column
{
    /// <summary>
    /// Defines the column as type blob
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "blob";
}

/// <summary>
/// A column that holds IP address values
/// </summary>
public class IPAddressColumn : Column
{
    /// <summary>
    /// Defines the column as type inet
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "inet";
}

/// <summary>
/// A column that holds duration values
/// </summary>
public class DurationColumn : Column
{
    /// <summary>
    /// Defines the column as type duration
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "duration";
}

/// <summary>
/// A column that holds dictionary/map values
/// </summary> 
public class DictionaryColumn : Column
{
    /// <summary>
    /// Defines the column as type map
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "map";

    /// <summary>
    /// The type of the keys in the dictionary
    /// </summary>
    [JsonPropertyName("keyType")]
    public string KeyType { get; set; }

    /// <summary>
    /// The type of the values in the dictionary
    /// </summary>
    [JsonPropertyName("valueType")]
    public string ValueType { get; set; }

    internal DictionaryColumn(string keyType, string valueType)
    {
        KeyType = keyType;
        ValueType = valueType;
    }

    /// <summary>
    /// Creates a new instance of the <see cref="DictionaryColumn"/> class.
    /// </summary>
    public DictionaryColumn() { }
}

/// <summary>
/// A column that holds list/array values
/// </summary>
public class ListColumn : Column
{
    /// <summary>
    /// Defines the column as type list
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "list";

    /// <summary>
    /// The type of the values in the list
    /// </summary>
    [JsonPropertyName("valueType")]
    public string ValueType { get; set; }

    internal ListColumn(string valueType)
    {
        ValueType = valueType;
    }

    /// <summary>
    /// Creates a new instance of the <see cref="ListColumn"/> class.
    /// </summary>
    public ListColumn() { }
}

/// <summary>
/// A column that holds set values
/// </summary>
public class SetColumn : Column
{
    /// <summary>
    /// Defines the column as type set
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "set";

    /// <summary>
    /// The type of the values in the set
    /// </summary>
    [JsonPropertyName("valueType")]
    public string ValueType { get; set; }

    internal SetColumn(string valueType)
    {
        ValueType = valueType;
    }

    /// <summary>
    /// Creates a new instance of the <see cref="SetColumn"/> class.
    /// </summary>
    public SetColumn() { }
}

/// <summary>
/// A column that holds vector values
/// </summary>
public class VectorColumn : Column
{
    /// <summary>
    /// Defines the column as type vector
    /// </summary>
    [JsonPropertyName("type")]
    public override string Type => "vector";

    /// <summary>
    /// The dimension of the vector
    /// </summary>
    [JsonPropertyName("dimension")]
    public int Dimension { get; set; }

    internal VectorColumn(int dimension)
    {
        Dimension = dimension;
    }

    /// <summary>
    /// Creates a new instance of the <see cref="VectorColumn"/> class.
    /// </summary>
    public VectorColumn() { }
}

/// <summary>
/// A column that holds vector values that are generated by a vectorization service
/// </summary>
public class VectorizeColumn : VectorColumn
{
    /// <summary>
    /// The vectorization service options
    /// </summary>
    [JsonPropertyName("service")]
    public VectorServiceOptions ServiceOptions { get; set; }

    /// <summary>
    /// Creates a new instance of the <see cref="VectorizeColumn"/> class with the specified dimension and service options.
    /// </summary>
    /// <param name="dimension"></param>
    /// <param name="serviceOptions"></param>
    public VectorizeColumn(int dimension, VectorServiceOptions serviceOptions) : base(dimension)
    {
        ServiceOptions = serviceOptions;
    }

    /// <summary>
    /// Creates a new instance of the <see cref="VectorizeColumn"/> class with the specified service options.
    /// </summary>
    /// <param name="serviceOptions"></param>
    public VectorizeColumn(VectorServiceOptions serviceOptions)
    {
        ServiceOptions = serviceOptions;
    }

    /// <summary>
    /// Creates a new instance of the <see cref="VectorizeColumn"/> class.
    /// </summary>
    public VectorizeColumn() { }
}