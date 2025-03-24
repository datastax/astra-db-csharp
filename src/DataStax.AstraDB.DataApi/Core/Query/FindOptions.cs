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

using MongoDB.Bson;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class FindOptions<T>
{
    [JsonIgnore]
    public SortBuilder<T> Sort { get; set; }

    [JsonIgnore]
    public IProjectionBuilder Projection { get; set; }

    [JsonIgnore]
    public int? Skip { get; set; }

    [JsonIgnore]
    public int? Limit { get; set; }

    [JsonIgnore]
    public bool? IncludeSimilarity { get; set; }

    [JsonIgnore]
    public bool? IncludeSortVector { get; set; }

    [JsonIgnore]
    internal Filter<T> Filter { get; set; }

    [JsonIgnore]
    internal string PageState { get; set; }

    [JsonInclude]
    [JsonPropertyName("filter")]
    internal Dictionary<string, object> FilterMap => Filter == null ? null : SerializeFilter(Filter);

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("sort")]
    internal Dictionary<string, object> SortMap => Sort == null ? null : Sort.Sorts.ToDictionary(x => x.Name, x => x.Value);

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("projection")]
    internal Dictionary<string, object> ProjectionMap => Projection == null ? null : Projection.Projections.ToDictionary(x => x.FieldName, x => x.Value);

    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonPropertyName("options")]
    internal FindApiOptions Options
    {
        get
        {
            if (IncludeSimilarity == null &&
                IncludeSortVector == null &&
                PageState == null &&
                Skip == null &&
                Limit == null &&
                string.IsNullOrEmpty(PageState))
            {
                return null;
            }
            return new FindApiOptions
            {
                IncludeSimilarity = IncludeSimilarity,
                IncludeSortVector = IncludeSortVector,
                PageState = PageState,
                Skip = Skip,
                Limit = Limit
            };
        }
    }

    private Dictionary<string, object> SerializeFilter(Filter<T> filter)
    {
        var result = new Dictionary<string, object>();
        if (filter.Value is Filter<T>[] filtersArray)
        {
            var serializedArray = new List<object>();
            foreach (var nestedFilter in filtersArray)
            {
                serializedArray.Add(SerializeFilter(nestedFilter));
            }
            result[filter.Name.ToString()] = serializedArray;
        }
        else
        {
            //TODO: abstract out ObjectId handling
            result[filter.Name.ToString()] = filter.Value is Filter<T> nestedFilter ? SerializeFilter(nestedFilter) :
              filter.Value is ObjectId ? filter.Value.ToString() : filter.Value;
        }
        return result;
    }
}

internal class FindApiOptions
{
    [JsonPropertyName("skip")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Skip { get; set; }

    [JsonPropertyName("limit")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Limit { get; set; }

    [JsonPropertyName("includeSimilarity")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? IncludeSimilarity { get; set; }

    [JsonPropertyName("includeSortVector")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? IncludeSortVector { get; set; }

    [JsonPropertyName("pageState")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string PageState { get; set; }
}
