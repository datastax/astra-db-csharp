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

namespace DataStax.AstraDB.DataApi.Core.Results;

/// <summary>
/// The result object for an operation returning a list of collections.
/// </summary>
public class ListCollectionsResult
{
    /// <summary>
    /// An array of <see cref="CollectionInfo"/> describing the collections.
    /// </summary>
    [JsonPropertyName("collections")]
    public CollectionInfo[] Collections { get; set; }
}