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

public class SimpleObjectWithLexical
{
    [DocumentId]
    [ColumnPrimaryKey]
    public int? Id { get; set; }
    public string Name { get; set; }
    [DocumentMapping(DocumentMappingField.Lexical)]
    [LexicalOptions(
        TokenizerName = "standard",
        Filters = new[] { "lowercase", "stop", "porterstem", "asciifolding" },
        CharacterFilters = new string[] { }
    )]
    public string LexicalValue => Name;
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

[TableName("bookTestTable")]
public class RowBook
{
    [ColumnPrimaryKey(1)]
    public string Title { get; set; }
    [ColumnVectorize(1024,
        serviceProvider: "nvidia",
        serviceModelName: "NV-Embed-QA")]
    public object Author { get; set; }
    [ColumnPrimaryKey(2)]
    public int NumberOfPages { get; set; }
    public DateTime? DueDate { get; set; }
    public HashSet<string> Genres { get; set; }
    public float Rating { get; set; }
}

[TableName("bookTestTableSinglePrimaryKey")]
public class RowBookSinglePrimaryKey
{
    [ColumnPrimaryKey(1)]
    public string Title { get; set; }
    public string Author { get; set; }
    public int NumberOfPages { get; set; }
    public DateTime DueDate { get; set; }
    public HashSet<string> Genres { get; set; }
    public float Rating { get; set; }
}

public class RowEventByDay
{
    [ColumnPrimaryKey(1)]
    [ColumnName("event_date")]
    public DateTime EventDate { get; set; }

    [ColumnPrimaryKey(2)]
    [ColumnName("id")]
    public Guid Id { get; set; }

    [ColumnName("title")]
    public string Title { get; set; }

    [ColumnName("location")]
    public string Location { get; set; }

    [ColumnName("category")]
    public string Category { get; set; }
}

public class RowBookWithSimilarity : RowBook
{
    [DocumentMapping(DocumentMappingField.Similarity)]
    public double Similarity { get; set; }
}

[TableName("testTable")]
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
    public Duration Duration { get; set; }
}

[TableName("testTable")]
public class ArrayTestRow
{
    [ColumnPrimaryKey(1)]
    public int Id { get; set; }
    public string[]? StringArray { get; set; }
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

#nullable enable
public class Book
{
    [DocumentId]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Guid? Id { get; set; }

    [JsonPropertyName("title")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Title { get; set; }

    [JsonPropertyName("author")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Author { get; set; }

    [JsonPropertyName("number_of_pages")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? NumberOfPages { get; set; }

    [JsonPropertyName("isCheckedOut")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? IsCheckedOut { get; set; }

    [DocumentMapping(DocumentMappingField.Vectorize)]
    public string? StringToVectorize { get; set; }
}


public class TestDataBook
{
    // This table uses a composite primary key
    // with 'title' as the first column in the key
    [ColumnPrimaryKey(1)]
    [ColumnName("title")]
    public string Title { get; set; } = null!;

    // This table uses a composite primary key
    // with 'author' as the second column in the key
    [ColumnPrimaryKey(2)]
    [ColumnName("author")]
    public string Author { get; set; } = null!;

    [ColumnName("number_of_pages")]
    public int? NumberOfPages { get; set; }

    [ColumnName("rating")]
    public float? Rating { get; set; }

    [ColumnName("publication_year")]
    public int? PublicationYear { get; set; }

    [ColumnName("summary")]
    public string? Summary { get; set; }

    [ColumnName("genres")]
    public HashSet<string>? Genres { get; set; }

    [ColumnName("metadata")]
    public Dictionary<string, string>? Metadata { get; set; }

    [ColumnName("is_checked_out")]
    public bool? IsCheckedOut { get; set; }

    [ColumnName("borrower")]
    public string? Borrower { get; set; }

    [ColumnName("due_date")]
    public DateTime? DueDate { get; set; }

    // This column will store vector embeddings.
    // The column will use an embedding model from NVIDIA to generate the
    // vector embeddings when data is inserted to the column. 
    [ColumnVectorize(
      1024,
      serviceProvider: "nvidia",
      serviceModelName: "NV-Embed-QA"
    )]
    [ColumnName("summary_genres_vector")]
    public object? SummaryGenresVector { get; set; }
}

public class DateTypeTest
{
    [ColumnPrimaryKey()]
    public int Id { get; set; }
    public DateTime Timestamp { get; set; }
    public DateOnly Date { get; set; }
    public TimeOnly Time { get; set; }
    public DateTime? MaybeTimestamp { get; set; }
    public DateOnly? MaybeDate { get; set; }
    public TimeOnly? MaybeTime { get; set; }
    public DateTime TimestampWithKind { get; set; }
}

public class UdtTest
{
    [ColumnPrimaryKey()]
    public int Id { get; set; }
    public TypesTester Udt { get; set; }
    public List<SimpleUdt> UdtList { get; set; }
}

public class UdtTestMinimal
{
    [ColumnPrimaryKey()]
    public int Id { get; set; }
    public SimpleUdtTwo Udt { get; set; }
}

[UserDefinedType()]
public class SimpleUdt
{
    public int Number { get; set; }
    public string Name { get; set; }
}

[UserDefinedType()]
public class SimpleUdtTwo
{
    public int Number { get; set; }
    public string Name { get; set; }
}

[UserDefinedType()]
public class TypesTester
{
    public string String { get; set; }
    //public System.Net.IPAddress Inet { get; set; }
    public int Int { get; set; }
    public byte TinyInt { get; set; }
    public short SmallInt { get; set; }
    public long BigInt { get; set; }
    public decimal Decimal { get; set; }
    public double Double { get; set; }
    public float Float { get; set; }
    public bool Boolean { get; set; }
    public Guid UUID { get; set; }
    public Duration Duration { get; set; }
    public DateTime Timestamp { get; set; }
    public DateOnly Date { get; set; }
    public TimeOnly Time { get; set; }
    public DateTime? MaybeTimestamp { get; set; }
    public DateOnly? MaybeDate { get; set; }
    public TimeOnly? MaybeTime { get; set; }
    public DateTime TimestampWithKind { get; set; }
}