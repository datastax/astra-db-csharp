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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Command options specific to Table get/set.
/// </summary>
public class DatabaseTableCommandOptions : DatabaseCommandOptions
{
    /// <summary>
    /// When specified, the client will send the x-embedding-api-key header with the specified key to any underlying HTTP request that requires vectorize authentication.
    /// </summary>
    public string EmbeddingApiKey
    {
        get
        {
            return AdditionalHeaders.TryGetValue("x-embedding-api-key", out var value) ? value : null;
        }
        set
        {
            AdditionalHeaders["x-embedding-api-key"] = value;
        }
    }
}
