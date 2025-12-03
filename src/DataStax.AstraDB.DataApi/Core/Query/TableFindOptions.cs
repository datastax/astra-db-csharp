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

/// <summary>
/// A set of options to be used when finding a row in a table.
/// </summary>
/// <typeparam name="T">The type of the row in the table.</typeparam>
public class TableFindOptions<T> : FindOptions<T, TableSortBuilder<T>>
{
    /// <summary>
    /// The builder used to define the sort to apply when running the query.
    /// </summary>
    [JsonIgnore]
    public override TableSortBuilder<T> Sort { get; set; }
}
