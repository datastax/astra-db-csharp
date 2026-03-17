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
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Results;

/// <summary>
/// The result of listing index names for a table.
/// </summary>
public class ListTableIndexNamesResult
{
    /// <summary>
    /// The names of the indexes defined on the table.
    /// </summary>
    [JsonPropertyName("indexes")]
    public List<string> IndexNames { get; set; }
}
