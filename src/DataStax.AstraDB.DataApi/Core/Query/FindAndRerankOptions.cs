/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

internal class FindAndRerankOptions<T>
{

    internal string RerankOn { get; set; }
    internal bool? IncludeScores { get; set; }
    internal bool? IncludeSortVector { get; set; }
    internal string RerankQuery { get; set; }
    internal int? Limit { get; set; }
    internal Dictionary<string, int> HybridLimits { get; set; }
    internal Filter<T> Filter { get; set; }
    internal IProjectionBuilder Projection { get; set; }
    internal List<Sort> Sorts { get; set; } = new();

    [JsonInclude]
    [JsonPropertyName("filter")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    private Dictionary<string, object> FilterMap => Filter?.Serialize();

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("sort")]
    private Dictionary<string, object> SortMap => Sorts?.ToDictionary(x => x.Name, x => x.Value);

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("projection")]
    private Dictionary<string, object> ProjectionMap => Projection?.Projections?.ToDictionary(x => x.FieldName, x => x.Value);

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("options")]
    private Dictionary<string, object> SerializableOptions
    {
        get
        {
            var options = new Dictionary<string, object>()
            {
                { "includeScores", IncludeScores },
                { "includeSortVector", IncludeSortVector },
                { "rerankOn", RerankOn },
                { "rerankQuery", RerankQuery },
                { "limit", Limit },
                { "hybridLimits", HybridLimits }
            };
            options = options.Where(pair => pair.Value != null).ToDictionary(pair => pair.Key, pair => pair.Value);
            if (options.Count == 0)
            {
                return null;
            }
            return options;
        }
    }

    internal FindAndRerankOptions<T> Clone()
    {
        var clone = new FindAndRerankOptions<T>
        {
            RerankOn = RerankOn,
            IncludeScores = IncludeScores,
            IncludeSortVector = IncludeSortVector,
            RerankQuery = RerankQuery,
            Limit = Limit,
            HybridLimits = HybridLimits != null ? new Dictionary<string, int>(HybridLimits) : null,
            Filter = Filter?.Clone(),
            Projection = Projection?.Clone(),
            Sorts = Sorts?.Select(s => s.Clone()).ToList() ?? new List<Sort>()
        };
        return clone;
    }

}
