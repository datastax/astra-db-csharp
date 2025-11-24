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
/// Standard text analyzers
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<TextAnalyzer>))]
public enum TextAnalyzer
{
    /// <summary>
    /// Filters StandardTokenizer output that divides text into terms on word boundaries and then uses the LowerCaseFilter.
    /// </summary>
    [JsonStringEnumMemberName("standard")]
    Standard,
    /// <summary>
    /// Filters LetterTokenizer output that divides text into terms whenever it encounters a character which is not a letter and then uses the LowerCaseFilter.
    /// </summary>
    [JsonStringEnumMemberName("simple")]
    Simple,
    /// <summary>
    /// Analyzer that uses WhitespaceTokenizer to divide text into terms whenever it encounters any whitespace character.
    /// </summary>
    [JsonStringEnumMemberName("whitespace")]
    Whitespace,
    /// <summary>
    /// Filters LetterTokenizer output with LowerCaseFilter and removes Lucene’s default English stop words.
    /// </summary>
    [JsonStringEnumMemberName("stop")]
    Stop,
    /// <summary>
    /// Normalizes input by applying LowerCaseFilter (no additional tokenization is performed).
    /// </summary>
    [JsonStringEnumMemberName("lowercase")]
    Lowercase,
    /// <summary>
    /// Analyzer that uses KeywordTokenizer, which is an identity function ("noop") on input values and tokenizes the entire input as a single token.
    /// </summary>
    [JsonStringEnumMemberName("keyword")]
    Keyword
}

