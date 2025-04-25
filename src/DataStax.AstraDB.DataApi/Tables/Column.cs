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


[JsonConverter(typeof(ColumnConverter))]
public abstract class Column
{
    [JsonIgnore]
    public abstract string Type { get; }
}

public class TextColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "text";
}

public class GuidColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "uuid";
}

public class IntColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "int";
}

public class LongColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "bigint";
}

public class DecimalColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "decimal";
}

public class FloatColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "float";
}

public class DoubleColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "double";
}

public class BooleanColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "boolean";
}

public class DateColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "timestamp";
}

public class BlobColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "blob";
}

public class IPAddressColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "inet";
}

public class DictionaryColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "map";

    [JsonPropertyName("keyType")]
    public string KeyType { get; set; }

    [JsonPropertyName("valueType")]
    public string ValueType { get; set; }

    internal DictionaryColumn(string keyType, string valueType)
    {
        KeyType = keyType;
        ValueType = valueType;
    }

    public DictionaryColumn() { }
}

public class ListColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "list";

    [JsonPropertyName("valueType")]
    public string ValueType { get; set; }

    internal ListColumn(string valueType)
    {
        ValueType = valueType;
    }

    public ListColumn() { }
}

public class SetColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "set";

    [JsonPropertyName("valueType")]
    public string ValueType { get; set; }

    internal SetColumn(string valueType)
    {
        ValueType = valueType;
    }

    public SetColumn() { }
}

public class VectorColumn : Column
{
    [JsonPropertyName("type")]
    public override string Type => "vector";

    [JsonPropertyName("dimension")]
    public int Dimension { get; set; }

    internal VectorColumn(int dimension)
    {
        Dimension = dimension;
    }

    public VectorColumn() { }
}

public class VectorizeColumn : VectorColumn
{
    [JsonPropertyName("service")]
    public VectorServiceOptions ServiceOptions { get; set; }

    public VectorizeColumn(int dimension, VectorServiceOptions serviceOptions) : base(dimension)
    {
        ServiceOptions = serviceOptions;
    }

    public VectorizeColumn() { }
}