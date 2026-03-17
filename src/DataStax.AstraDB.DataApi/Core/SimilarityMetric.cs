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

namespace DataStax.AstraDB.DataApi.Core;


/// <summary>
/// The vector similarity metric used to compare embeddings during vector search.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<SimilarityMetric>))]
public enum SimilarityMetric
{
    /// <summary>Cosine similarity, measuring the angle between vectors. Well suited for normalized embeddings.</summary>
    [JsonStringEnumMemberName("cosine")]
    Cosine,
    /// <summary>Euclidean distance, measuring the straight-line distance between vectors.</summary>
    [JsonStringEnumMemberName("euclidean")]
    Euclidean,
    /// <summary>Dot product similarity, equivalent to cosine similarity when vectors are unit-normalized.</summary>
    [JsonStringEnumMemberName("dot_product")]
    DotProduct
}

