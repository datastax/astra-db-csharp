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

/// <summary>
/// The main entrypoint into working with the Data API. It sits at the top of the conceptual hierarchy of the SDK.
/// The client can be passed a default token, which can be overridden by a stronger/weaker token when connectiong to a Database or Admin instance.
/// 
/// The DataApiClient, and the related methods for interacting with the database, accepts a set of options that can be used to affect the 
/// command execution. These options can be specified at any level in the call hierarchy (Client, Database, Collection, Command, etc.) 
/// The most specific defined option (or it's default) will be used for each request.
/// 
/// </summary>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="DataApiClient"/> class.
    /// 
    /// When using this constructor, the token must be provided to the <see cref="GetDatabase"/> or 
    /// <see cref="GetAstraDatabasesAdmin"/> methods,
    /// or to the eventual end commands via a <see cref="CommandOptions"/> parameter.
    /// </summary>
    public DataApiClient() : this(null, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataApiClient"/> class with a default authentication token. 
    /// This token can be overridden when getting a database <see cref="GetDatabase"/> or admin instance <see cref="GetAstraDatabasesAdmin"/>
    /// as well as in the <see cref="CommandOptions"/> parameter of the commands.
    /// </summary>
    /// <param name="token">The token to use for authentication.</param>
    public DataApiClient(string token)
        : this(token, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataApiClient"/> class with a default set of options
    /// When using this constructor, the token must be provided in the <see cref="CommandOptions"/> parameter,
    /// to the <see cref="GetDatabase"/> or <see cref="GetAstraDatabasesAdmin"/> methods,
    /// or the eventual end commands via a <see cref="CommandOptions"/> parameter.
    /// </summary>
    /// <param name="options">The default options to use for commands executed by this client.</param>
    public DataApiClient(CommandOptions options)
        : this(null, options)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataApiClient"/> class with a default authentication token.
    /// When using the default constructor, the token must be provided to the <see cref="GetDatabase"/> or <see cref="GetAstraDatabasesAdmin"/> methods
    /// or the eventual end commands via a <see cref="CommandOptions"/> parameter.
    /// </summary>
    /// <param name="token">The token to use for authentication.</param>
    /// <param name="options">The default options to use for commands executed by this client.</param>
    /// <param name="logger">The logger to use for logging.</param>
    public DataApiClient(string token, CommandOptions options, ILogger logger = null)
    {
        _options = options ?? new CommandOptions();
        _options.Token = token;
        _logger = logger ?? NullLogger.Instance;

        var services = new ServiceCollection();
        services.AddHttpClient();
        _serviceProvider = services.BuildServiceProvider();

        _httpClientFactory = _serviceProvider.GetService<IHttpClientFactory>()!;
    }

    /// <summary>
    /// Gets an instance of the <see cref="AstraDatabasesAdmin"/> class for administration of Astra databases.
    /// Options (including token) from the <see cref="DataApiClient"/> will be passed to the <see cref="AstraDatabasesAdmin"/>
    /// and can be overridden by the <see cref="CommandOptions"/> parameter of the commands.
    /// </summary>
    /// <returns>An admin instance of the <see cref="AstraDatabasesAdmin"/> class.</returns>
    public AstraDatabasesAdmin GetAstraDatabasesAdmin()
    {
        return GetAstraDatabasesAdmin(null as CommandOptions);
    }

    /// <summary>
    /// Gets an instance of the <see cref="AstraDatabasesAdmin"/> class for administration of Astra databases.
    /// Options from the <see cref="DataApiClient"/> will be passed to the <see cref="AstraDatabasesAdmin"/>
    /// and can be overridden by the <see cref="CommandOptions"/> parameter of the commands.
    /// The <see cref="superAdminToken"/> parameter is used to override the token from the <see cref="DataApiClient"/>
    /// with a more specific token as needed for security purposes.
    /// </summary>
    /// <param name="superAdminToken">The super admin token to use for authentication.</param>
    /// <returns>An admin instance of the <see cref="AstraDatabasesAdmin"/> class.</returns>
    public AstraDatabasesAdmin GetAstraDatabasesAdmin(string superAdminToken)
    {
        return GetAstraDatabasesAdmin(new CommandOptions() { Token = superAdminToken });
    }

    /// <summary>
    /// Gets an instance of the <see cref="AstraDatabasesAdmin"/> class.
    /// 
    /// Any options provided in the <see cref="adminOptions"/> parameter will take precedence over the options from the <see cref="DataApiClient"/>.
    /// </summary>
    /// <param name="adminOptions">The options to use for the admin instance.</param>
    /// <returns>An admin instance of the <see cref="AstraDatabasesAdmin"/> class.</returns>
    public AstraDatabasesAdmin GetAstraDatabasesAdmin(CommandOptions adminOptions)
    {
        var applicableOptions = CommandOptions.Merge(_options, adminOptions);
        var applicableDestination = applicableOptions.Destination;
        Guard.Equals(applicableDestination, DataApiDestination.ASTRA, "Destinations other than ASTRA cannot be used with GetAstraAdmin. Please check your Destination settings for the DataApiClient or the overload with adminOptions");
        var applicableToken = applicableOptions.Token;
        Guard.NotNullOrEmpty(applicableToken, nameof(adminOptions.Token), "Token must be provided to the DataApiClient constructor or to a GetAstraAdmin() overload.");
        return new AstraDatabasesAdmin(this, adminOptions);
    }

    /// <summary>
    /// Gets an instance of the <see cref="Database"/> class given the API Endpoint for the database.
    /// 
    /// The default keyspace will be used. If you need to connect to a different keyspace, use the <see cref="GetDatabase(string, string)"/> overload
    /// or set the keyspace on the <see cref="DatabaseOptions"/> parameter and use the <see cref="GetDatabase(string, DatabaseOptions)"/> overload.
    /// </summary>
    /// <param name="apiEndpoint">The API endpoint of the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataApiClient("token");
    /// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com");
    /// </code>
    /// </example>
    public Database GetDatabase(string apiEndpoint)
    {
        return GetDatabase(apiEndpoint, null as DatabaseOptions);
    }

    /// <summary>
    /// Gets an instance of the <see cref="Database"/> class given the API Endpoint and the keyspace to connect to.
    /// </summary>
    /// <param name="apiEndpoint">The API endpoint of the database.</param>
    /// <param name="keyspace">The keyspace to connect to.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataApiClient("token");
    /// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com", "myKeyspace");
    /// </code>
    /// </example>
    public Database GetDatabase(string apiEndpoint, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return GetDatabase(apiEndpoint, dbOptions);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> given the API Endpoint and a set of options.
    /// 
    /// Any options provided in the <see cref="dbOptions"/> parameter will take precedence over the options from the <see cref="DataApiClient"/>.
    /// </summary>
    /// <param name="apiEndpoint">The API endpoint of the database.</param>
    /// <param name="dbOptions">The options to use for the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataApiClient("token");
    /// var database = client.GetDatabase("https://1ae8dd5d-19ce-452d-9df8-6e5b78b82ca7-us-east1.apps.astra.datastax.com", new DatabaseOptions() { Keyspace = "myKeyspace" });
    /// </code>
    /// </example>
    public Database GetDatabase(string apiEndpoint, DatabaseOptions dbOptions)
    {
        return new Database(apiEndpoint, this, dbOptions);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataApiClient("token");
    /// var database = client.GetDatabase(Guid.Parse("databaseId"));
    /// </code>
    /// </example>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string)">).
    /// </remarks>
    public Database GetDatabase(Guid databaseId)
    {
        return GetDatabase(databaseId, null as DatabaseOptions);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, set to the provided keyspace.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="keyspace">The keyspace to use for the database commands.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataApiClient("token");
    /// var database = client.GetDatabase(Guid.Parse("databaseId"), "keyspace");
    /// </code>
    /// </example>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, string)">).
    /// </remarks>
    public Database GetDatabase(Guid databaseId, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return GetDatabase(databaseId, dbOptions);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> using the Guid database Id.
    /// 
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string)">).
    /// </remarks>
    public async Task<Database> GetDatabaseAsync(Guid databaseId)
    {
        return await GetDatabaseAsync(databaseId, new DatabaseOptions(), false).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, using the provided options.
    /// 
    /// Any options provided in the <see cref="dbOptions"/> parameter will take precedence over the options from the <see cref="DataApiClient"/>.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="dbOptions">The options to use for the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, DatabaseOptions)">).
    /// </remarks>
    public Database GetDatabase(Guid databaseId, DatabaseOptions dbOptions)
    {
        return GetDatabaseAsync(databaseId, dbOptions, true).ResultSync();
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, using the provided keyspace.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="keyspace">The keyspace to use for the database commands.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, string)">).
    /// </remarks>
    public async Task<Database> GetDatabaseAsync(Guid databaseId, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return await GetDatabaseAsync(databaseId, dbOptions, false).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, using the provided options.
    /// 
    /// Any options provided in the <see cref="dbOptions"/> parameter will take precedence over the options from the <see cref="DataApiClient"/>.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="dbOptions">The options to use for the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, DatabaseOptions)">).
    /// </remarks>
    public async Task<Database> GetDatabaseAsync(Guid databaseId, DatabaseOptions dbOptions)
    {
        return await GetDatabaseAsync(databaseId, dbOptions, false).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> using the database Id.
    /// 
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <exception cref="ArgumentException">Thrown when the database Id is not a valid Guid.</exception>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string)">).
    /// </remarks>
    public Database GetDatabaseById(string databaseId)
    {
        return GetDatabaseById(databaseId, null as DatabaseOptions);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, using the provided keyspace.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="keyspace">The keyspace to use for the database commands.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <exception cref="ArgumentException">Thrown when the database Id is not a valid Guid.</exception>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, string)">).
    /// </remarks>
    public Database GetDatabaseById(string databaseId, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return GetDatabaseById(databaseId, dbOptions);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, using the provided options.
    /// 
    /// Any options provided in the <see cref="dbOptions"/> parameter will take precedence over the options from the <see cref="DataApiClient"/>.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="dbOptions">The options to use for the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <exception cref="ArgumentException">Thrown when the database Id is not a valid Guid.</exception>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, DatabaseOptions)">).
    /// </remarks>
    public Database GetDatabaseById(string databaseId, DatabaseOptions dbOptions)
    {
        return GetDatabaseByIdAsync(databaseId, dbOptions, true).ResultSync();
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> using the database Id.
    /// 
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <exception cref="ArgumentException">Thrown when the database Id is not a valid Guid.</exception>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string)">).
    /// </remarks>
    public Task<Database> GetDatabaseByIdAsync(string databaseId)
    {
        return GetDatabaseByIdAsync(databaseId, new DatabaseOptions(), false);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, using the provided keyspace.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="keyspace">The keyspace to use for the database commands.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <exception cref="ArgumentException">Thrown when the database Id is not a valid Guid.</exception>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, string)">).
    /// </remarks>
    public Task<Database> GetDatabaseByIdAsync(string databaseId, string keyspace)
    {
        var dbOptions = new DatabaseOptions() { Keyspace = keyspace };
        return GetDatabaseByIdAsync(databaseId, dbOptions, false);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> based on the database Id, using the provided options.
    /// 
    /// Any options provided in the <see cref="dbOptions"/> parameter will take precedence over the options from the <see cref="DataApiClient"/>.
    /// </summary>
    /// <param name="databaseId">The Guid of the database.</param>
    /// <param name="dbOptions">The options to use for the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <exception cref="ArgumentException">Thrown when the database Id is not a valid Guid.</exception>
    /// <remarks>
    ///      Using a Guid instead of the Database API endpoint requires an extra API call to lookup the appropriate database.
    ///      If you want to avoid this, use an overload that accepts the API endpoint (<see cref="GetDatabase(string, DatabaseOptions)">).
    /// </remarks>
    public Task<Database> GetDatabaseByIdAsync(string databaseId, DatabaseOptions dbOptions)
    {
        return GetDatabaseByIdAsync(databaseId, dbOptions, false);
    }

    private Task<Database> GetDatabaseByIdAsync(string databaseId, DatabaseOptions dbOptions, bool runSynchronously)
    {
        var parsed = Guid.TryParse(databaseId, out var guid);
        if (!parsed)
        {
            throw new ArgumentException("Invalid database Id");
        }
        return GetDatabaseAsync(guid, dbOptions, runSynchronously);
    }

    private async Task<Database> GetDatabaseAsync(Guid databaseId, DatabaseOptions dbOptions, bool runSynchronously)
    {
        DatabaseInfo dbInfo;
        if (runSynchronously)
        {
            dbInfo = GetAstraDatabasesAdmin().GetDatabaseInfoAsync(databaseId, runSynchronously).ResultSync();
        }
        else
        {
            dbInfo = await GetAstraDatabasesAdmin().GetDatabaseInfoAsync(databaseId).ConfigureAwait(false);
        }
        var apiEndpoint = $"https://{dbInfo.Id}-{dbInfo.Info.Region}.apps.astra.datastax.com";
        return GetDatabase(apiEndpoint, dbOptions);
    }
}
