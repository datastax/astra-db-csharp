using System.Collections.Generic;
using System.Text.Json.Serialization;

internal class DropUserDefinedTypeRequest
{
    [JsonInclude]
    [JsonPropertyName("name")]
    internal string Name { get; set; }

    [JsonInclude]
    [JsonPropertyName("options")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    internal Dictionary<string, object> Options { get; set; }

    internal void SetIfExists(bool ifExists)
    {
        var optionsKey = "ifExists";
        if (!ifExists)
        {
            if (Options != null)
            {
                Options.Remove(optionsKey);
            }
        }
        else
        {
            Options ??= new Dictionary<string, object>();
            Options[optionsKey] = ifExists;
        }
    }
}