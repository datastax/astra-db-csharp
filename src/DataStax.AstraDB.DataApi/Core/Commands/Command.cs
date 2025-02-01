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

using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace DataStax.AstraDB.DataApi.Core.Commands;

public class Command
{
    private readonly ILogger _logger;
    private readonly CommandOptions _commandOptions;
    private readonly DataApiClient _client;
    private readonly CommandUrlBuilder _urlBuilder;
    private readonly string _name;
    private List<String> _urlPaths = new();

    internal object Payload { get; set; }
    internal string UrlPostfix { get; set; }


    internal Command(DataApiClient client, CommandOptions[] options, CommandUrlBuilder urlBuilder) : this(null, client, options, urlBuilder)
    {

    }

    internal Command(string name, DataApiClient client, CommandOptions[] options, CommandUrlBuilder urlBuilder)
    {
        //TODO include database-specific options (and maybe collection-specific as well)
        _commandOptions = CommandOptions.Merge(options);
        _client = client;
        _name = name;
        _logger = client.Logger;
        _urlBuilder = urlBuilder;
    }

    public Command WithDocument(object document)
    {
        Payload = new { document };
        return this;
    }

    public Command WithPayload(object document)
    {
        Payload = document;
        return this;
    }

    public Command AddUrlPath(string path)
    {
        _urlPaths.Add(path);
        return this;
    }

    public object BuildContent()
    {
        if (string.IsNullOrEmpty(_name))
        {
            return Payload;
        }
        var dictionary = new Dictionary<string, object>
        {
            { _name, Payload }
        };
        return dictionary;
    }

    internal async Task<ApiResponse<ApiResponseDictionary>> RunAsync(bool runSynchronously)
    {
        return await RunAsync<ApiResponseDictionary>(runSynchronously).ConfigureAwait(false);
    }

    internal async Task<ApiResponse<TStatus>> RunAsync<TStatus>(bool runSynchronously)
    {
        return await RunCommandAsync<ApiResponse<TStatus>>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);
    }

    internal async Task<T> RunAsyncRaw<T>(bool runSynchronously)
    {
        return await RunAsyncRaw<T>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);
    }

    internal async Task<T> RunAsyncRaw<T>(HttpMethod httpMethod, bool runSynchronously)
    {
        return await RunCommandAsync<T>(httpMethod, runSynchronously).ConfigureAwait(false);
    }

    private async Task<T> RunCommandAsync<T>(HttpMethod method, bool runSynchronously)
    {
        var content = new StringContent(JsonSerializer.Serialize(BuildContent()), Encoding.UTF8, "application/json");
        var url = _urlBuilder.BuildUrl();
        if (_urlPaths.Any())
        {
            url += "/" + string.Join("/", _urlPaths);
        }

        await MaybeLogRequestDebug(url, content, runSynchronously).ConfigureAwait(false);

        var httpClient = _client.HttpClientFactory.CreateClient();
        var request = new HttpRequestMessage()
        {
            Method = method,
            RequestUri = new Uri(url),
            Content = method == HttpMethod.Get ? null : content,
        };
        //TODO: add Database-level options (and Collection, etc.);

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _commandOptions.Token);
        request.Headers.Add("Token", _commandOptions.Token);

        string responseContent = null;
        HttpResponseMessage response = null;

        //TODO implement rest of options (timeout, dbenvironment, etc.)
        if (runSynchronously)
        {
#if NET5_0_OR_GREATER
            response = httpClient.Send(request);
            var contentTask = Task.Run(() => response.Content.ReadAsStringAsync());
            contentTask.Wait();
            responseContent = contentTask.Result;
#else
            var requestTask = Task.Run(() => httpClient.SendAsync(request));
            requestTask.Wait();
            response = requestTask.Result;
            var contentTask = Task.Run(() => response.Content.ReadAsStringAsync());
            contentTask.Wait();
            responseContent = contentTask.Result;
#endif
        }
        else
        {
            response = await httpClient.SendAsync(request).ConfigureAwait(false);
            responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        }

        MaybeLogDebugMessage("Response Status Code: {StatusCode}", response.StatusCode);
        MaybeLogDebugMessage("Content: {Content}", responseContent);

        //TODO try/catch
        return JsonSerializer.Deserialize<T>(responseContent);
    }

    private void MaybeLogDebugMessage(string message, params object[] args)
    {
        if (_client.ClientOptions.RunMode == RunMode.Debug)
        {
            _logger.LogInformation(message, args);
        }
    }

    private async Task MaybeLogRequestDebug(string url, StringContent content, bool runSynchronously)
    {
        if (_client.ClientOptions.RunMode == RunMode.Debug)
        {
            _logger.LogInformation("Url: {Url}", url);
            _logger.LogInformation("Additional Headers:");
            string data;
            if (runSynchronously)
            {
                var task = Task.Run(() => content.ReadAsStringAsync());
                task.Wait();
                data = task.Result;
            }
            else
            {
                data = await content.ReadAsStringAsync().ConfigureAwait(false);
            }
            _logger.LogInformation("Data: {Data}", data);
        }
    }
}
