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
/// A warning returned by the Data API, for example to indicate query limitations or deprecated usage.
/// </summary>
internal class ApiWarning
{
    /// <summary>
    /// The human-readable warning message.
    /// </summary>
    [JsonPropertyName("message")]
    public string Message { get; set; }

    /// <summary>
    /// The warning code.
    /// </summary>
    [JsonPropertyName("code")]
    public string Code { get; set; }

    /// <summary>
    /// The error code associated with the warning.
    /// </summary>
    [JsonPropertyName("errorCode")]
    public string ErrorCode { get; set; }

    /// <summary>
    /// The name of the exception class that generated the warning.
    /// </summary>
    [JsonPropertyName("exceptionClass")]
    public string ExceptionClass { get; set; }

    /// <summary>
    /// The warning family category.
    /// </summary>
    [JsonPropertyName("family")]
    public string Family { get; set; }

    /// <summary>
    /// The scope in which the warning applies.
    /// </summary>
    [JsonPropertyName("scope")]
    public string Scope { get; set; }

    /// <summary>
    /// A short title describing the warning.
    /// </summary>
    [JsonPropertyName("title")]
    public string Title { get; set; }

    /// <summary>
    /// A unique identifier for the warning.
    /// </summary>
    [JsonPropertyName("id")]
    public string Id { get; set; }
}