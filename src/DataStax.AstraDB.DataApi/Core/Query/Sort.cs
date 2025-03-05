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

using DataStax.AstraDB.DataApi.Core.Commands;

namespace DataStax.AstraDB.DataApi.Core.Query;

public class Sort
{
    internal Sort(string sortKey, object value)
    {
        Name = sortKey;
        Value = value;
    }

    internal string Name { get; set; }
    internal object Value { get; set; }

    public static Sort Ascending(string field) => new(field, SortOrder.Ascending);

    public static Sort Descending(string field) => new(field, SortOrder.Descending);

    public static Sort Vector(float[] vector) => new(DataApiKeywords.Vector, vector);

    public static Sort Vectorize(string valueToVectorize) => new(DataApiKeywords.Vectorize, valueToVectorize);
}
