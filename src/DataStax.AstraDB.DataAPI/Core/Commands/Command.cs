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

namespace DataStax.AstraDB.DataAPI.Core.Commands;

public class Command
{
    private readonly ILogger _logger;

    internal string Name { get; set; }
    internal object Payload { get; set; }
    internal Database Database { get; set; }
    internal string UrlPostfix { get; set; }

    public Command(Database database, string urlPostfix, string name)
    {
        Database = database;
        UrlPostfix = urlPostfix;
        Name = name;
        _logger = database.Client.Logger;
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

    public object BuildContent()
    {
        var dictionary = new Dictionary<string, object>
        {
            { Name, Payload }
        };
        return dictionary;
    }

    internal async Task<ApiResponse<ApiResponseDictionary>> RunAsync(bool runSynchronous = false)
    {
        return await RunAsync<ApiResponseDictionary>(runSynchronous);
    }

    internal async Task<ApiResponse<T>> RunAsync<T>(bool runSynchronous = false)
    {
        var content = new StringContent(JsonSerializer.Serialize(BuildContent()), Encoding.UTF8, "application/json");
        var url = BuildUrl();

        await MaybeLogRequestDebug(url, content, runSynchronous);

        var httpClient = Database.Client.HttpClientFactory.CreateClient();
        var request = new HttpRequestMessage()
        {
            Method = HttpMethod.Post,
            RequestUri = new Uri(url),
            Content = content
        };
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Database.Client.Token);
        request.Headers.Add("Token", Database.Client.Token);

        string responseContent = null;
        HttpResponseMessage response = null;

        //TODO implement rest of options (timeout, dbenvironment, etc.)
        if (runSynchronous)
        {
#if NET5_0_OR_GREATER
            response = httpClient.Send(request);
            responseContent = response.Content.ReadAsStringAsync().Result;
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
            response = await httpClient.SendAsync(request);
            responseContent = await response.Content.ReadAsStringAsync();
        }

        MaybeLogDebugMessage("Response Status Code: {StatusCode}", response.StatusCode);
        MaybeLogDebugMessage("Content: {Content}", responseContent);

        //TODO try/catch
        return JsonSerializer.Deserialize<ApiResponse<T>>(responseContent);
    }

    private string BuildUrl()
    {
        var url = $"{Database.ApiEndpoint}/api/json/{Database.Client.ClientOptions.ApiVersion.ToUrlString()}" +
            $"/{Database.DatabaseOptions.CurrentKeyspace}/{UrlPostfix}";
        return url;
    }

    private void MaybeLogDebugMessage(string message, params object[] args)
    {
        if (Database.Client.ClientOptions.RunMode == RunMode.Debug)
        {
            _logger.LogInformation(message, args);
        }
    }

    private async Task MaybeLogRequestDebug(string url, StringContent content, bool runSynchronous)
    {
        if (Database.Client.ClientOptions.RunMode == RunMode.Debug)
        {
            _logger.LogInformation("Url: {Url}", url);
            _logger.LogInformation("Additional Headers:");
            foreach (var header in Database.Client.ClientOptions.AdditionalHeaders)
            {
                _logger.LogInformation("  {Key}: {Value}", header.Key, string.Join(", ", header.Value));
            }
            _logger.LogInformation("Method: POST");
            string data;
            if (runSynchronous)
            {
                var task = Task.Run(() => content.ReadAsStringAsync());
                task.Wait();
                data = task.Result;
            }
            else
            {
                data = await content.ReadAsStringAsync();
            }
            _logger.LogInformation("Data: {Data}", data);
        }
    }
}
