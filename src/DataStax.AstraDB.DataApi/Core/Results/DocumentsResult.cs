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
/// A list of documents returned from an operation.
/// </summary>
/// <typeparam name="T">The type of the documents</typeparam>
public class DocumentsResult<T>
{
    /// <summary>
    /// The list of documents returned.
    /// </summary>
    [JsonPropertyName("documents")]
    public List<T> Documents { get; set; }

    [JsonInclude]
    [JsonPropertyName("nextPageState")]
    internal string NextPageState { get; set; }
}