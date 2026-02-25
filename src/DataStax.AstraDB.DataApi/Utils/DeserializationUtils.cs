
using System.Linq;
using System.Text.Json;

namespace DataStax.AstraDB.DataApi.Utils;

internal static class DeserializationUtils
{
    internal static object UnwrapJsonElement(JsonElement je)
    {
        return je.ValueKind switch
        {
            JsonValueKind.String => je.GetString(),
            JsonValueKind.Number => je.TryGetInt64(out var l) ? l : je.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => je.EnumerateArray()
                .Select(UnwrapJsonElement)
                .ToArray(),
            JsonValueKind.Object => je.EnumerateObject()
                .ToDictionary(
                    p => p.Name,
                    p => UnwrapJsonElement(p.Value)),
            _ => je.GetRawText()
        };
    }
}