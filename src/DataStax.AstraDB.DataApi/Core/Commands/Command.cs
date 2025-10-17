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

using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.SerDes;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi.Core.Commands;

internal class Command
{
    private readonly ILogger _logger;
    private readonly List<CommandOptions> _commandOptionsTree;
    private readonly DataApiClient _client;
    private readonly CommandUrlBuilder _urlBuilder;
    private readonly string _name;
    private List<string> _urlPaths = new();

    internal object Payload { get; set; }
    internal string UrlPostfix { get; set; }
    internal TimeoutManager TimeoutManager { get; set; } = new TimeoutManager();

    internal readonly struct EmptyResult { }

    private Func<HttpResponseMessage, Task> _responseHandler;
    internal Func<HttpResponseMessage, Task> ResponseHandler { set { _responseHandler = value; } }

    internal Command(DataApiClient client, CommandOptions[] options, CommandUrlBuilder urlBuilder) : this(null, client, options, urlBuilder)
    {

    }

    internal Command(string name, DataApiClient client, CommandOptions[] options, CommandUrlBuilder urlBuilder)
    {
        _commandOptionsTree = options.ToList();
        _client = client;
        _name = name;
        _logger = client.Logger;
        _urlBuilder = urlBuilder;
    }

    internal Command AddCommandOptions(CommandOptions options)
    {
        if (options != null)
        {
            _commandOptionsTree.Add(options);
        }
        return this;
    }

    internal Command WithPayload(object document)
    {
        Payload = document;
        return this;
    }

    internal Command WithTimeoutManager(TimeoutManager timeoutManager)
    {
        TimeoutManager = timeoutManager;
        return this;
    }

    internal Command AddUrlPath(string path)
    {
        _urlPaths.Add(path);
        return this;
    }

    internal object BuildContent()
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

    internal async Task<ApiResponseWithStatus<ApiResponseDictionary>> RunAsyncReturnDictionary(bool runSynchronously)
    {
        return await RunAsyncReturnStatus<ApiResponseDictionary>(runSynchronously).ConfigureAwait(false);
    }

    internal async Task<ApiResponseWithStatus<TStatus>> RunAsyncReturnStatus<TStatus>(bool runSynchronously)
    {
        var response = await RunCommandAsync<ApiResponseWithStatus<TStatus>>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);
        if (response.Errors != null && response.Errors.Count > 0)
        {
            throw new CommandException(response.Errors);
        }
        return response;
    }

    internal async Task<ApiResponseWithData<TData, TStatus>> RunAsyncReturnDocumentData<TData, TDocument, TStatus>(bool runSynchronously)
    {
        var useDocumentConverter = typeof(TDocument) != typeof(Document);
        if (useDocumentConverter)
        {
            _commandOptionsTree.Add(new CommandOptions()
            {
                OutputConverter = new DocumentConverter<TDocument>()
            });
        }
        var response = await RunCommandAsync<ApiResponseWithData<TData, TStatus>>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);
        if (response.Errors != null && response.Errors.Count > 0)
        {
            throw new CommandException(response.Errors);
        }
        return response;
    }

    internal async Task<ApiResponseWithData<TData, TStatus>> RunAsyncReturnData<TData, TStatus>(bool runSynchronously)
    {
        var response = await RunCommandAsync<ApiResponseWithData<TData, TStatus>>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);
        if (response.Errors != null && response.Errors.Count > 0)
        {
            throw new CommandException(response.Errors);
        }
        return response;
    }

    internal async Task<T> RunAsyncRaw<T>(bool runSynchronously)
    {
        return await RunAsyncRaw<T>(HttpMethod.Post, runSynchronously).ConfigureAwait(false);
    }

    internal async Task<T> RunAsyncRaw<T>(HttpMethod httpMethod, bool runSynchronously)
    {
        return await RunCommandAsync<T>(httpMethod, runSynchronously).ConfigureAwait(false);
    }

    internal string Serialize<T>(T input, JsonSerializerOptions serializeOptions = null, bool log = false)
    {
        var commandOptions = CommandOptions.Merge(_commandOptionsTree.ToArray());
        serializeOptions ??= new JsonSerializerOptions();
        serializeOptions.Converters.Add(new ObjectIdConverter());
        serializeOptions.Converters.Add(new DurationConverter());
        serializeOptions.Converters.Add(new ByteArrayAsBinaryJsonConverter());
        if (commandOptions.SerializeDateAsDollarDate == true)
        {
            serializeOptions.Converters.Add(new DateTimeConverter<DateTimeOffset>());
            serializeOptions.Converters.Add(new DateTimeConverter<DateTime>());
        }
        if (commandOptions.SerializeGuidAsDollarUuid == true)
        {
            serializeOptions.Converters.Add(new GuidConverter());
        }
        serializeOptions.Converters.Add(new IpAddressConverter());
        if (commandOptions.InputConverter != null)
        {
            serializeOptions.Converters.Add(commandOptions.InputConverter);
        }
        if (log)
        {
            _logger.LogInformation("Serializing {Type} with options {Options}", typeof(T).Name, serializeOptions);
            _logger.LogInformation("Input: {Input}", input);
        }
        var serialized = JsonSerializer.Serialize(input, serializeOptions);
        if (log)
        {
            _logger.LogInformation("Output: {Output}", serialized);
        }
        return serialized;
    }

    internal T Deserialize<T>(string input)
    {
        var commandOptions = CommandOptions.Merge(_commandOptionsTree.ToArray());
        var deserializeOptions = new JsonSerializerOptions();
        deserializeOptions.Converters.Add(new DurationConverter());
        deserializeOptions.Converters.Add(new ByteArrayAsBinaryJsonConverter());
        if (commandOptions.OutputConverter != null)
        {
            deserializeOptions.Converters.Add(commandOptions.OutputConverter);
        }
        deserializeOptions.Converters.Add(new ObjectIdConverter());
        if (commandOptions.SerializeGuidAsDollarUuid == true)
        {
            deserializeOptions.Converters.Add(new GuidConverter());
        }
        if (commandOptions.SerializeDateAsDollarDate == true)
        {
            deserializeOptions.Converters.Add(new DateTimeConverter<DateTimeOffset>());
            deserializeOptions.Converters.Add(new DateTimeConverter<DateTime>());
        }
        deserializeOptions.Converters.Add(new IpAddressConverter());
        deserializeOptions.Converters.Add(new AnalyzerOptionsConverter());

        return JsonSerializer.Deserialize<T>(input, deserializeOptions);
    }

    private async Task<T> RunCommandAsync<T>(HttpMethod method, bool runSynchronously)
    {
        var commandOptions = CommandOptions.Merge(_commandOptionsTree.ToArray());
        var content = new StringContent(Serialize(BuildContent()), Encoding.UTF8, "application/json");

        var url = _urlBuilder.BuildUrl(commandOptions);
        if (_urlPaths.Any())
        {
            // Join the URL parts, ensuring that no additional slashes are introduced
            url += "/" + string.Join("/", _urlPaths.Select(part => part.Trim('/')));
        }

        await MaybeLogRequestDebug(url, content, runSynchronously).ConfigureAwait(false);

        HttpClient httpClient;
#if NET5_0_OR_GREATER || NETCOREAPP2_1_OR_GREATER || NET472_OR_GREATER
        {
            var handler = new SocketsHttpHandler()
            {
                AllowAutoRedirect = commandOptions.HttpClientOptions.FollowRedirects
            };
            var connectTimeout = TimeoutManager.GetConnectionTimeout(commandOptions);
            if (connectTimeout.TotalMilliseconds != 0)
            {
                handler.ConnectTimeout = connectTimeout;
            }

            httpClient = new HttpClient(handler);
        }
#else 
        {
            var handler = new HttpClientHandler
            {
                AllowAutoRedirect = commandOptions.HttpClientOptions.FollowRedirects
            };
            httpClient = new HttpClient(handler);
        }
#endif

        var request = new HttpRequestMessage()
        {
            Method = method,
            RequestUri = new Uri(url),
            Content = method == HttpMethod.Get ? null : content
        };
        if (!runSynchronously)
        {
            request.Version = commandOptions.HttpClientOptions.HttpVersion;
        }

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", commandOptions.Token);
        request.Headers.Add("Token", commandOptions.Token);

        string responseContent = null;
        HttpResponseMessage response = null;

        var ctsForTimeout = new CancellationTokenSource();
        var requestTimeout = TimeoutManager.GetRequestTimeout(commandOptions);
        if (requestTimeout.Milliseconds > 0)
        {
            ctsForTimeout.CancelAfter(requestTimeout);
        }
        var cancellationTokenForTimeout = ctsForTimeout.Token;

        List<CancellationToken> cancellationTokens = new();
        cancellationTokens.Add(cancellationTokenForTimeout);
        if (commandOptions.CancellationToken != null)
        {
            cancellationTokens.Add(commandOptions.CancellationToken.Value);
        }
        if (commandOptions.BulkOperationCancellationToken != null)
        {
            cancellationTokens.Add(commandOptions.BulkOperationCancellationToken.Value);
        }
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationTokens.ToArray());
        try
        {
            if (runSynchronously)
            {
#if NET5_0_OR_GREATER
                response = httpClient.Send(request, linkedCts.Token);
                var contentTask = Task.Run(() => response.Content.ReadAsStringAsync());
                contentTask.Wait();
                responseContent = contentTask.Result;
#else
                var requestTask = Task.Run(() => httpClient.SendAsync(request, linkedCts.Token));
                requestTask.Wait();
                response = requestTask.Result;
                var contentTask = Task.Run(() => response.Content.ReadAsStringAsync());
                contentTask.Wait();
                responseContent = contentTask.Result;
#endif
            }
            else
            {
                response = await httpClient.SendAsync(request, linkedCts.Token).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode == System.Net.HttpStatusCode.GatewayTimeout ||
                        response.StatusCode == System.Net.HttpStatusCode.RequestTimeout)
                    {
                        throw new TimeoutException($"Request to timed out. Consider increasing the RequestTimeout settings using the CommandOptions parameter.");
                    }
                    else if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                    {
                        throw new UnauthorizedAccessException("Unauthorized access. Please check your token.");
                    }
                    else
                    {
                        responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        MaybeLogDebugMessage("Response Status Code: {StatusCode}", response.StatusCode);
                        MaybeLogDebugMessage("Content: {Content}", responseContent);
                        throw new HttpRequestException($"Request failed with status code {response.StatusCode}.");
                    }
                }
                responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            }
        }
        catch (TaskCanceledException ex)
        {
            if (commandOptions.BulkOperationCancellationToken != null && commandOptions.BulkOperationCancellationToken.Value.IsCancellationRequested)
            {
                throw new TimeoutException($"Bulk operation timed out after {TimeoutManager.GetBulkOperationTimeout(commandOptions).TotalSeconds} seconds. Consider increasing the timeout using the CommandOptions.TimeoutOptions.BulkOperationTimeout parameter.", ex);
            }
            if (cancellationTokenForTimeout.IsCancellationRequested)
            {
                throw new TimeoutException($"HTTP request timed out after {requestTimeout.TotalSeconds} seconds. Consider increasing the timeout using the CommandOptions.TimeoutOptions.RequestTimeout parameter.", ex);
            }
            throw; // Other cancellation sources (e.g., commandOptions.CancellationToken)
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "HTTP request failed.");
            throw;
        }
        finally
        {
            httpClient.Dispose();
        }
        if (_responseHandler != null)
        {
            if (runSynchronously)
            {
                _responseHandler(response).ResultSync();
            }
            else
            {
                await _responseHandler(response);
            }
        }

        MaybeLogDebugMessage("Response Status Code: {StatusCode}", response.StatusCode);
        MaybeLogDebugMessage("Content: {Content}", responseContent);

        MaybeLogDebugMessage("Raw Response: {Response}", response);

        if (string.IsNullOrEmpty(responseContent))
        {
            return default;
        }

        return Deserialize<T>(responseContent);
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
