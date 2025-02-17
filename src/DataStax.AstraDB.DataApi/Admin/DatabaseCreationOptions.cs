using DataStax.AstraDB.DataApi.Core;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Admin;

public class DatabaseCreationOptions
{
    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("cloudProvider")]
    public CloudProviderType CloudProvider { get; set; } = CloudProviderType.GCP;

    [JsonPropertyName("region")]
    public string Region { get; set; } = "us-east1";

    [JsonPropertyName("keyspace")]
    public string Keyspace { get; set; } = Database.DefaultKeyspace;

    [JsonPropertyName("capacityUnits")]
    public int CapacityUnits { get; set; } = 1;

    [JsonPropertyName("tier")]
    public string Tier { get; set; } = "serverless";
}