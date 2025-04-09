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

using System;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// Options for the HTTP client used by the Data API.
/// </summary>
public class HttpClientOptions
{
    /// <summary>
    /// The version of HTTP to use.
    /// </summary>
    public Version HttpVersion { get; set; } = new Version(2, 0);

    /// <summary>
    /// Whether the HTTP client should follow redirects or not.
    /// </summary>
    public bool FollowRedirects { get; set; } = true;
}