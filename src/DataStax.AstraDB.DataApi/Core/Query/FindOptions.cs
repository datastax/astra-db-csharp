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
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class FindOptions<T>
{
    [JsonInclude]
    [JsonPropertyName("filter")]
    internal Filter<T>? Filter { get; set; }
    [JsonIgnore]
    public List<Sort> Sort { get; set; } = new List<Sort>();
    [JsonIgnore]
    public List<Projection> Projection { get; set; } = new List<Projection>();
    [JsonIgnore]
    public int? Skip { get; set; }
    [JsonIgnore]
    public int? Limit { get; set; }
    [JsonIgnore]
    public bool? IncludeSimilarity { get; set; }
    [JsonIgnore]
    public bool? IncludeSortVector { get; set; }
    [JsonIgnore]
    public string PageState { get; set; }

    [JsonInclude]
    [JsonPropertyName("sort")]
    internal Dictionary<string, object> SortMap => Sort == null ? null : Sort.ToDictionary(x => x.Name, x => x.Value);
    [JsonInclude]
    [JsonPropertyName("projection")]
    internal Dictionary<string, object> ProjectionMap => Projection == null ? null : Projection.ToDictionary(x => x.Field, x => x.Value);
    [JsonInclude]
    [JsonPropertyName("options")]
    internal object Options => new { includeSimilarity = IncludeSimilarity, includeSortVector = IncludeSortVector }; //, skip = Skip, limit = Limit, pageState = PageState };
}


