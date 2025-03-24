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

using DataStax.AstraDB.DataApi.Admin;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Net.Http;
using System;
using System.Threading.Tasks;

namespace DataStax.AstraDB.DataApi;

public class DataApiClient
{
    private readonly CommandOptions _options;
    private readonly ServiceProvider _serviceProvider;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger _logger;

    internal CommandOptions ClientOptions => _options;
    internal ServiceProvider ServiceProvider => _serviceProvider;
    internal IHttpClientFactory HttpClientFactory => _httpClientFactory;
    internal ILogger Logger => _logger;

    public DataApiClient(string token)
        : this(token, new CommandOptions())
    {
    }

    public DataApiClient(CommandOptions options)
        : this(null, options)
    {
    }

    public DataApiClient(string token, CommandOptions options, ILogger logger = null)
    {
        options.Token = token;
        Guard.NotNull(options, nameof(options));
        _options = options;
        _logger = logger ?? NullLogger.Instance;

        var services = new ServiceCollection();
        services.AddHttpClient();
        _serviceProvider = services.BuildServiceProvider();

        _httpClientFactory = _serviceProvider.GetService<IHttpClientFactory>()!;
    }

    public AstraDatabasesAdmin GetAstraAdmin(CommandOptions adminOptions)
    {
        var applicableOptions = CommandOptions.Merge(_options, adminOptions);
        var applicableDestination = applicableOptions.Destination;
        Guard.Equals(applicableDestination, DataApiDestination.ASTRA, "Destinations other than ASTRA cannot be used with GetAstraAdmin. Please check your Destination settings for the DataApiClient or the overload with adminOptions");
        var applicableToken = applicableOptions.Token;
        Guard.NotNullOrEmpty(applicableToken, nameof(adminOptions.Token), "Token must be provided to the DataApiClient constructor or to a GetAstraAdmin() overload.");
        return new AstraDatabasesAdmin(this, adminOptions);
    }

    public AstraDatabasesAdmin GetAstraAdmin()
    {
        return GetAstraAdmin(new CommandOptions());
    }

    public AstraDatabasesAdmin GetAstraAdmin(string superAdminToken)
    {
        return GetAstraAdmin(new CommandOptions() { Token = superAdminToken });
    }

    public Database GetDatabase(string apiEndpoint)
    {
        return GetDatabase(apiEndpoint, null as DatabaseOptions);
    }

    public Database GetDatabase(string apiEndpoint, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return GetDatabase(apiEndpoint, dbOptions);
    }

    public Database GetDatabase(string apiEndpoint, DatabaseOptions dbOptions)
    {
        return new Database(apiEndpoint, this, dbOptions);
    }

    public Database GetDatabase(Guid databaseId)
    {
        return GetDatabase(databaseId, null as DatabaseOptions);
    }

    public Database GetDatabase(Guid databaseId, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return GetDatabase(databaseId, dbOptions);
    }

    public Database GetDatabase(Guid databaseId, DatabaseOptions dbOptions)
    {
        return GetDatabaseAsync(databaseId, dbOptions, true).ResultSync();
    }

    public async Task<Database> GetDatabaseAsync(Guid databaseId)
    {
        return await GetDatabaseAsync(databaseId, new DatabaseOptions(), false).ConfigureAwait(false);
    }

    public async Task<Database> GetDatabaseAsync(Guid databaseId, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return await GetDatabaseAsync(databaseId, dbOptions, false).ConfigureAwait(false);
    }

    public async Task<Database> GetDatabaseAsync(Guid databaseId, DatabaseOptions dbOptions)
    {
        return await GetDatabaseAsync(databaseId, dbOptions, false).ConfigureAwait(false);
    }

    private async Task<Database> GetDatabaseAsync(Guid databaseId, DatabaseOptions dbOptions, bool runSynchronously)
    {
        DatabaseInfo dbInfo;
        if (runSynchronously)
        {
            dbInfo = GetAstraAdmin().GetDatabaseInfoAsync(databaseId, runSynchronously).ResultSync();
        }
        else
        {
            dbInfo = await GetAstraAdmin().GetDatabaseInfoAsync(databaseId).ConfigureAwait(false);
        }
        var apiEndpoint = $"https://{dbInfo.Id}-{dbInfo.Info.Region}.apps.astra.datastax.com";
        return GetDatabase(apiEndpoint, dbOptions);
    }
}
