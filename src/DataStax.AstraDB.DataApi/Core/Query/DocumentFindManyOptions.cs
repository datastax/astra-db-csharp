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

using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Query;

internal class DocumentFindManyOptions<T> : DocumentFindOptions<T>, IFindManyOptions<T, DocumentSortBuilder<T>>
{
    [JsonIgnore]
    public int? Skip { get => _skip; set => _skip = value; }

    [JsonIgnore]
    public int? Limit { get => _limit; set => _limit = value; }

    [JsonIgnore]
    internal bool? IncludeSortVector { get => _includeSortVector; set => _includeSortVector = value; }

    bool? IFindManyOptions<T, DocumentSortBuilder<T>>.IncludeSortVector { get => IncludeSortVector; set => IncludeSortVector = value; }

    IFindManyOptions<T, DocumentSortBuilder<T>> IFindManyOptions<T, DocumentSortBuilder<T>>.Clone()
    {
        var clone = new DocumentFindManyOptions<T>
        {
            Filter = Filter != null ? Filter.Clone() : null,
            PageState = PageState,
            Skip = Skip,
            Limit = Limit,
            IncludeSortVector = IncludeSortVector,
            IncludeSimilarity = IncludeSimilarity,
            Projection = Projection != null ? Projection.Clone() : null,
            Sort = Sort != null ? Sort.Clone() : null
        };
        return clone;
    }

}
