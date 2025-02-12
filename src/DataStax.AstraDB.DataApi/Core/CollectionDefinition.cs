using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core;

public class CollectionDefinition
{
    [JsonPropertyName("defaultId")]
    public DefaultIdOptions DefaultId { get; set; }

    [JsonPropertyName("vector")]
    public VectorOptions Vector { get; set; }

    [JsonPropertyName("indexing")]
    public IndexingOptions Indexing { get; set; }
}




