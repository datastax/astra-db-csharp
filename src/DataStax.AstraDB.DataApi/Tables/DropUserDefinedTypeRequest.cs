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

    internal void SetSkipIfExists(bool skipIfExists)
    {
        var optionsKey = "ifNotExists";
        if (!skipIfExists)
        {
            if (Options != null)
            {
                Options.Remove(optionsKey);
            }
        }
        else
        {
            Options ??= new Dictionary<string, object>();
            Options[optionsKey] = skipIfExists;
        }
    }
}