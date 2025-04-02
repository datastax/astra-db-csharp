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

public class CommandOptions
{
    internal DBEnvironment? Environment { get; set; }
    internal RunMode? RunMode { get; set; }
    internal string Keyspace { get; set; }
    internal JsonConverter InputConverter { get; set; }
    internal JsonConverter OutputConverter { get; set; }

    public string Token { get; internal set; }
    public DataApiDestination? Destination { get; set; }
    public HttpClientOptions HttpClientOptions { get; set; }
    public TimeoutOptions TimeoutOptions { get; set; }
    public ApiVersion? ApiVersion { get; set; }
    public CancellationToken? CancellationToken { get; set; }

    public void SetConvertersIfNull(JsonConverter inputConverter, JsonConverter outputConverter)
    {
        InputConverter ??= inputConverter;
        OutputConverter ??= outputConverter;
    }
    public bool IncludeKeyspaceInUrl { get; set; }

    public static CommandOptions Merge(params CommandOptions[] arr)
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


