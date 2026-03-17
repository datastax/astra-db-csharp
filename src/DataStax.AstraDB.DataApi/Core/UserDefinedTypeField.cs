namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Represents a field in a user-defined type (UDT), consisting of a name and a CQL type.
/// </summary>
public class UserDefinedTypeField
{
    /// <summary>The name of the UDT field.</summary>
    public string Name { get; set; }
    /// <summary>The CQL type of the UDT field.</summary>
    public string Type { get; set; }
}