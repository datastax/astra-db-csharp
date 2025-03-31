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

namespace DataStax.AstraDB.DataApi.Core;

internal class ApiResponseWithStatus<T>
{
    [JsonPropertyName("status")]
    public T Result { get; set; }

    [JsonPropertyName("errors")]
    public List<ApiError> Errors { get; set; }

    [JsonPropertyName("warnings")]
    public List<ApiWarning> Warnings { get; set; }
}

internal class ApiResponseWithData<T, TStatus>
{
    [JsonPropertyName("errors")]
    public List<ApiError> Errors { get; set; }

    [JsonPropertyName("warnings")]
    public List<ApiWarning> Warnings { get; set; }

    [JsonPropertyName("data")]
    public T Data { get; set; }

    [JsonPropertyName("status")]
    public TStatus Status { get; set; }
}


