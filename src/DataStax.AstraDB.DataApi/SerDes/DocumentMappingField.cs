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

namespace DataStax.AstraDB.DataApi.SerDes;

/// <summary>
/// Special field types
/// </summary>
public enum DocumentMappingField
{
    /// <summary>Serializes as "_id" for unique identifiers</summary>
    Id,
    /// <summary>Serializes as "$vectorize" for a string to vectorize</summary>
    Vectorize,
    /// <summary>Serializes as "$vector" for vector data.</summary>
    Vector,
    /// <summary>On read operations only, deserializes the similarity result for vector comparisons</summary>
    Similarity
}