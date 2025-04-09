

using DataStax.AstraDB.DataApi.SerDes;
using MongoDB.Bson;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

public class SimpleObjectWithVector
{
    [DocumentMapping(DocumentMappingField.Id)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Id { get; set; }
    public string Name { get; set; }
    [DocumentMapping(DocumentMappingField.Vector)]
    public float[] VectorEmbeddings { get; set; }
}

public class SimpleObjectWithVectorize
{
    [DocumentMapping(DocumentMappingField.Id)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
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
    [DocumentMapping(DocumentMappingField.Id)]
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
    [DocumentMapping(DocumentMappingField.Id)]
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