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

using DataStax.AstraDB.DataAPI.Core;
using DataStax.AstraDB.DataAPI.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Net.Http;

namespace DataStax.AstraDB.DataAPI;

public class DataAPIClient
{
    private readonly DataAPIClientOptions _options;
    private readonly string _token;
    private readonly ServiceProvider _serviceProvider;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger _logger;

    internal DataAPIClientOptions ClientOptions => _options;
    internal string Token => _token;
    internal ServiceProvider ServiceProvider => _serviceProvider;
    internal IHttpClientFactory HttpClientFactory => _httpClientFactory;
    internal ILogger Logger => _logger;

    public DataAPIClient(string token)
        : this(token, new DataAPIClientOptions())
    {
    }

    public DataAPIClient(string token, ILogger logger)
        : this(token, new DataAPIClientOptions(), logger)
    {
    }

    public DataAPIClient(string token, DataAPIClientOptions options, ILogger logger = null)
    {
        Guard.NotNullOrEmpty(token, nameof(token));
        Guard.NotNull(options, nameof(options));
        _token = token;
        _options = options;
        _options = options;
        _logger = logger;
        if (logger == null)
        {
            _logger = NullLogger.Instance;
        }

        var services = new ServiceCollection();
        services.AddHttpClient();
        _serviceProvider = services.BuildServiceProvider();

        _httpClientFactory = _serviceProvider.GetService<IHttpClientFactory>()!;

    }

    public Database GetDatabase(string apiEndpoint)
    {
        return GetDatabase(apiEndpoint, new DatabaseOptions());
    }

    public Database GetDatabase(string apiEndpoint, string keyspace)
    {
        var dbOptions = new DatabaseOptions(keyspace);
        return GetDatabase(apiEndpoint, dbOptions);
    }

    private Database GetDatabase(string apiEndpoint, DatabaseOptions dbOptions)
    {
        return new Database(apiEndpoint, dbOptions, this);
    }
}
