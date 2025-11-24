using System.Collections.Generic;
using System.Text.Json.Serialization;

public class AlterUserDefinedTypeDefinition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AlterUserDefinedTypeDefinition"/> class.
    /// </summary>
    /// <param name="name">The name of the User Defined Type to alter.</param>
    public AlterUserDefinedTypeDefinition(string name)
    {
        Name = name;
    }

    /// <summary>
    /// The name of the User Defined Type to alter.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }

    /// <summary>
    /// The fields to rename.
    /// </summary>
    [JsonPropertyName("rename")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public RenameFieldsDefinition Rename { get; set; }

    /// <summary>
    /// The fields to add.
    /// </summary>
    [JsonPropertyName("add")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public AddFieldsDefinition Add { get; set; }

    /// <summary>
    /// Adds a field to the User Defined Type.
    /// </summary>
    /// <param name="field">The field to add.</param>
    /// <returns>The current instance of <see cref="AlterUserDefinedTypeDefinition"/>.</returns>
    public AlterUserDefinedTypeDefinition AddField(string field, DataApiType fieldType)
    {
        Add ??= new AddFieldsDefinition();
        Add.Fields ??= new Dictionary<string, string>();
        Add.Fields.Add(field, fieldType.Key);
        return this;
    }

    /// <summary>
    /// Renames a field in the User Defined Type.
    /// </summary>
    /// <param name="fieldName">The name of the field to rename.</param>
    /// <param name="newFieldName">The new name of the field.</param>
    /// <returns>The current instance of <see cref="AlterUserDefinedTypeDefinition"/>.</returns>
    public AlterUserDefinedTypeDefinition RenameField(string fieldName, string newFieldName)
    {
        Rename ??= new RenameFieldsDefinition();
        Rename.Fields ??= new Dictionary<string, string>();
        Rename.Fields.Add(fieldName, newFieldName);
        return this;
    }
}

public class RenameFieldsDefinition
{
    [JsonPropertyName("fields")]
    public Dictionary<string, string> Fields { get; set; }
}

public class AddFieldsDefinition
{
    [JsonPropertyName("fields")]
    public Dictionary<string, string> Fields { get; set; }
}