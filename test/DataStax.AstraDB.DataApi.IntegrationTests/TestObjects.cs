using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.SerDes;
using DataStax.AstraDB.DataApi.Tables;
using MongoDB.Bson;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

public class SimpleObjectWithVector
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
    [DocumentMapping(DocumentMappingField.Vector)]
    public float[] VectorEmbeddings { get; set; }
}

public class SimpleObjectWithVectorize
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
    [DocumentMapping(DocumentMappingField.Vectorize)]
    public string StringToVectorize => Name;
}

public class SimpleObjectWithVectorizeResult : SimpleObjectWithVectorize
{
    [DocumentMapping(DocumentMappingField.Similarity)]
    public double? Similarity { get; set; }
}

public class SimpleObjectWithObjectId
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public ObjectId? _id { get; set; }
    public string Name { get; set; }
}

public class SimpleObjectWithGuidId
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Guid? _id { get; set; }
    public string Name { get; set; }
}

public class SimpleObject
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? _id { get; set; }
    public string Name { get; set; }
    public Properties Properties { get; set; }
}

public class SerializationTest
{
    [DocumentId]
    public int TestId { get; set; }
    public Properties NestedProperties { get; set; }
}

public class Properties
{
    public string PropertyOne { get; set; }
    public string PropertyTwo { get; set; }
    public int IntProperty { get; set; }
    public string[] StringArrayProperty { get; set; }
    public bool BoolProperty { get; set; }
    public DateTime DateTimeProperty { get; set; }
    public DateTimeOffset DateTimeOffsetProperty { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string SkipWhenNull { get; set; }
}

public class SimpleObjectSkipNulls
{
    public int _id { get; set; }
    public string Name { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string PropertyOne { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string PropertyTwo { get; set; }
}

public class DifferentIdsObject
{
    [DocumentId]
    public object TheId { get; set; }
    public string Name { get; set; }
}

public class Restaurant
{
    public Restaurant() { }
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string RestaurantId { get; set; }
    public string Cuisine { get; set; }
    public Address Address { get; set; }
    public string Borough { get; set; }
    public List<GradeEntry> Grades { get; set; }
}

public class Address
{
    public string Building { get; set; }
    public double[] Coordinates { get; set; }
    public string Street { get; set; }
    public string ZipCode { get; set; }
}

public class GradeEntry
{
    public DateTime Date { get; set; }
    public string Grade { get; set; }
    public float? Score { get; set; }
}

public class SimpleRowObject
{
    [ColumnPrimaryKey]
    public string Name { get; set; }
}

public class RowBook
{
    [ColumnPrimaryKey]
    public string Title { get; set; }
    public string Author { get; set; }
    public int NumberOfPages { get; set; }
    public DateTime DueDate { get; set; }
    public HashSet<string> Genres { get; set; }
    public float Rating { get; set; }
}

public class RowTestObject
{
    [ColumnPrimaryKey(1)]
    [ColumnName("renamed")]
    public string Name { get; set; }
    [ColumnPrimaryKey(2)]
    [ColumnVector(4)]
    public float[] Vector { get; set; }
    [ColumnPrimaryKey(3)]
    [ColumnVectorize(1024,
        serviceProvider: "nvidia",
        serviceModelName: "NV-Embed-QA")]
    public object StringToVectorize { get; set; }
    [ColumnPrimaryKey(4)]
    public string Text { get; set; }
    public System.Net.IPAddress Inet { get; set; }
    [ColumnPrimaryKey(5)]
    public int Int { get; set; }
    [ColumnPrimaryKey(6)]
    public byte TinyInt { get; set; }
    [ColumnPrimaryKey(7)]
    public short SmallInt { get; set; }
    [ColumnPrimaryKey(8)]
    public long BigInt { get; set; }
    [ColumnPrimaryKey(9)]
    public decimal Decimal { get; set; }
    [ColumnPrimaryKey(10)]
    public double Double { get; set; }
    [ColumnPrimaryKey(11)]
    public float Float { get; set; }
    public Dictionary<string, int> IntDictionary { get; set; }
    public Dictionary<string, decimal> DecimalDictionary { get; set; }
    public HashSet<string> StringSet { get; set; }
    public HashSet<int> IntSet { get; set; }
    public List<string> StringList { get; set; }
    //[JsonConverter(typeof(JsonStringConverter<List<Properties>>))]
    [ColumnJsonString]
    public List<Properties> ObjectList { get; set; }
    [ColumnPrimaryKey(12)]
    public bool Boolean { get; set; }
    [ColumnPrimaryKey(13)]
    public DateTime Date { get; set; }
    [ColumnPrimaryKey(14)]
    public Guid UUID { get; set; }
    public byte[] Blob { get; set; }
}

public class CompositePrimaryKey
{
    [ColumnPrimaryKey(2)]
    public string KeyTwo { get; set; }
    [ColumnPrimaryKey(1)]
    public string KeyOne { get; set; }
}

public class CompoundPrimaryKey
{
    [ColumnPrimaryKey(2)]
    public string KeyTwo { get; set; }
    [ColumnPrimaryKey(1)]
    public string KeyOne { get; set; }
    [ColumnPrimaryKeySort(2, SortDirection.Descending)]
    public string SortTwoDescending { get; set; }
    [ColumnPrimaryKeySort(1, SortDirection.Ascending)]
    public string SortOneAscending { get; set; }
}

public class BrokenCompositePrimaryKey
{
    [ColumnPrimaryKey(3)]
    public string KeyTwo { get; set; }
    [ColumnPrimaryKey(1)]
    public string KeyOne { get; set; }
}

public class BrokenCompoundPrimaryKey
{
    [ColumnPrimaryKey(2)]
    public string KeyTwo { get; set; }
    [ColumnPrimaryKey(1)]
    public string KeyOne { get; set; }
    [ColumnPrimaryKeySort(2, SortDirection.Descending)]
    public string SortTwoDescending { get; set; }
    [ColumnPrimaryKeySort(0, SortDirection.Ascending)]
    public string SortOneAscending { get; set; }
}

public class Book
{
    [DocumentMapping(DocumentMappingField.Id)]
    public Guid Id { get; set; }

    [DocumentMapping(DocumentMappingField.Vector)]
    public double[] Vector { get; set; }

    [JsonPropertyName("title")]
    public string Title { get; set; }

    [JsonPropertyName("isCheckedOut")]
    public bool IsCheckedOut { get; set; }

    [JsonPropertyName("numberOfPages")]
    public int NumberOfPages { get; set; }
}