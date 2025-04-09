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

using DataStax.AstraDB.DataApi.Utils;
using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// This class provides a set of low-level options to control the interactions with the underlying data store.
/// 
/// These options can be provided at any level of the SDK hierarchy:
///     <see cref="DataApiClient"/>
///         <see cref="Database"/>
///             <see cref="Collections.Collection"/>
///             
/// as well as directly to each of the methods. You can provide different options objects at each level,
/// the options specified at the most granular level will take precedence.
/// </summary>
public class CommandOptions
{
    internal DBEnvironment? Environment { get; set; }
    internal RunMode? RunMode { get; set; }
    internal string Keyspace { get; set; }
    internal JsonConverter InputConverter { get; set; }
    internal JsonConverter OutputConverter { get; set; }

    /// <summary>
    /// The token to use for authentication
    /// </summary>
    public string Token { get; internal set; }

    /// <summary>
    /// The destination datastore.
    /// 
    /// Defaults to <see cref="DataApiDestination.ASTRA"/>
    /// </summary>
    public DataApiDestination? Destination { get; set; }

    /// <summary>
    /// Options for the HTTP client
    /// 
    /// Defaults to HttpVersion: 2.0, FollowRedirects: true
    /// </summary>
    public HttpClientOptions HttpClientOptions { get; set; }

    /// <summary>
    /// Connection and request timeout options
    /// 
    /// Defaults to ConnectTimeoutMillis: 5000, RequestTimeoutMillis: 30000
    /// </summary>
    public TimeoutOptions TimeoutOptions { get; set; }

    /// <summary>
    /// API version to connect to
    /// 
    /// Defaults to <see cref="ApiVersion.V1"/>
    /// </summary>
    public ApiVersion? ApiVersion { get; set; }

    /// <summary>
    /// An optional CancellationToken to interrupt asynchronous operations
    /// </summary>
    public CancellationToken? CancellationToken { get; set; }

    internal void SetConvertersIfNull(JsonConverter inputConverter, JsonConverter outputConverter)
    {
        InputConverter ??= inputConverter;
        OutputConverter ??= outputConverter;
    }

    internal bool IncludeKeyspaceInUrl { get; set; }

    internal static CommandOptions Merge(params CommandOptions[] arr)
    {
        var list = arr.Where(o => o != null).ToList();
        list.Insert(0, Defaults());

        bool? FirstNonNull(Func<CommandOptions, bool?> selector) =>
            list.Select(selector).LastOrDefault(v => v != null);

        var options = new CommandOptions
        {
            Token = list.Select(o => o.Token).Merge(),
            Environment = list.Select(o => o.Environment).Merge(),
            RunMode = list.Select(o => o.RunMode).Merge(),
            Destination = list.Select(o => o.Destination).Merge(),
            HttpClientOptions = list.Select(o => o.HttpClientOptions).Merge(),
            TimeoutOptions = list.Select(o => o.TimeoutOptions).Merge(),
            ApiVersion = list.Select(o => o.ApiVersion).Merge(),
            CancellationToken = list.Select(o => o.CancellationToken).Merge(),
            Keyspace = list.Select(o => o.Keyspace).Merge(),
            InputConverter = list.Select(o => o.InputConverter).Merge(),
            OutputConverter = list.Select(o => o.OutputConverter).Merge(),
            IncludeKeyspaceInUrl = FirstNonNull(x => x.IncludeKeyspaceInUrl) ?? Defaults().IncludeKeyspaceInUrl,
        };
        return options;
    }

    /// <summary>
    /// The default set of options
    /// </summary>
    /// <returns>Default command options</returns>
    public static CommandOptions Defaults()
    {
        return new CommandOptions()
        {
            Environment = DBEnvironment.Production,
            RunMode = Core.RunMode.Normal,
            Destination = DataApiDestination.ASTRA,
            ApiVersion = Core.ApiVersion.V1,
            HttpClientOptions = new HttpClientOptions(),
            Keyspace = Database.DefaultKeyspace,
            IncludeKeyspaceInUrl = true,
        };
    }
}


