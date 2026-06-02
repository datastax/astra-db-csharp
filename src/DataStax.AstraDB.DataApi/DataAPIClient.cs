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

namespace DataStax.AstraDB.DataApi;

/// <summary>
/// The main entrypoint into working with the Data API. It sits at the top of the conceptual hierarchy of the SDK.
/// The client can be passed a default token, which can be overridden by a stronger/weaker token when
/// connecting to a Database or Admin instance.
/// 
/// The DataAPIClient, and the related methods for interacting with the database, accepts a set of options
/// that can be used to affect the command execution. These options can be specified at any level in the
/// call hierarchy (Client, Database, Collection, Command, etc.) 
/// The most specific defined option (or its default) will be used for each request.
/// 
///
///  Once you have a <see cref="DataAPIClient"/> instance, 
///  you can use it to get a <see cref="Core.Database"/> instance.
///  From there you can create or connect to a <see cref="Collections.Collection"/>.
///  
/// </summary>
public class DataAPIClient
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
    /// Initializes a new instance of the <see cref="DataAPIClient"/> class.
    /// 
    /// When using this constructor, generally a token is provided later, to the <see cref="GetDatabase(string)"/> or 
    /// <see cref="GetAstraDatabasesAdmin(string, GetAstraDatabasesAdminOptions)"/> methods; or to the eventual
    /// end commands via a <see cref="CommandOptions"/> parameter.
    /// </summary>
    public DataAPIClient() : this(null, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataAPIClient"/> class with a default authentication token. 
    /// This token can be overridden when getting a database <see cref="GetDatabase(string)"/>
    /// or admin instance <see cref="GetAstraDatabasesAdmin(string, GetAstraDatabasesAdminOptions)"/>
    /// as well as in the <see cref="CommandOptions"/> parameter of the commands.
    /// </summary>
    /// <param name="token">The token to use for authentication.</param>
    public DataAPIClient(string token)
        : this(token, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataAPIClient"/> class with a default set of options
    ///
    /// When using this constructor, if not passing a token within the options, generally it is provided later,
    /// to the <see cref="GetDatabase(string)"/> or <see cref="GetAstraDatabasesAdmin(string, GetAstraDatabasesAdminOptions)"/>
    /// methods; or to the eventual end commands via a <see cref="CommandOptions"/> parameter.
    /// </summary>
    /// <param name="options">The default options to use for commands executed by this client.</param>
    public DataAPIClient(CommandOptions options)
        : this(null, options)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataAPIClient"/> class with a default authentication token.
    /// When using the default constructor, the token must be provided to the <see cref="GetDatabase(string)"/>
    /// or <see cref="GetAstraDatabasesAdmin(string, GetAstraDatabasesAdminOptions)"/> methods
    /// or the eventual end commands via a <see cref="CommandOptions"/> parameter.
    /// </summary>
    /// <param name="token">The token to use for authentication.</param>
    /// <param name="options">The default options to use for commands executed by this client.</param>
    /// <param name="logger">The logger to use for logging.</param>
    public DataAPIClient(string token, CommandOptions options, ILogger logger = null)
    {
        _options = options ?? new CommandOptions();
        _options.Token = token;
        _logger = logger ?? NullLogger.Instance;

        var services = new ServiceCollection();
        services.AddHttpClient();
        _serviceProvider = services.BuildServiceProvider();

        _httpClientFactory = _serviceProvider.GetService<IHttpClientFactory>()!;
    }

    /// <inheritdoc cref="GetAstraDatabasesAdmin(string, GetAstraDatabasesAdminOptions)"/>
    /// <param name="options">The options to use for the resulting databases admin.</param>
    public AstraDatabasesAdmin GetAstraDatabasesAdmin(GetAstraDatabasesAdminOptions options = null)
    {
        return GetAstraDatabasesAdmin(null, options);
    }

    /// <summary>
    /// Gets an instance of the <see cref="AstraDatabasesAdmin"/> class.
    /// 
    /// Any options explicitly provided will override those on this <see cref="DataAPIClient"/>.
    /// </summary>
    /// <param name="token">A token with administrative powers, to use for authentication if required. When passed, overrides any token in the 'options' parameter.</param>
    /// <param name="options">The options to use for the resulting databases admin.</param>
    /// <returns>An instance of <see cref="AstraDatabasesAdmin"/>.</returns>
    public AstraDatabasesAdmin GetAstraDatabasesAdmin(string token, GetAstraDatabasesAdminOptions options = null)
    {
        var applicableOptions = CommandOptions.Merge(_options, options, new GetAstraDatabasesAdminOptions() { Token = token });
        var applicableDestination = applicableOptions.Destination;
        Guard.Equals(applicableDestination, DataAPIDestination.ASTRA, "Destinations other than ASTRA cannot be used with GetAstraDatabasesAdmin. Please check the requested Destination.");
        return new AstraDatabasesAdmin(this, applicableOptions);
    }

    /// <summary>
    /// Gets an instance of the <see cref="Database"/> class given the API Endpoint for the database.
    /// 
    /// The default keyspace will be used. If you need to connect to a different keyspace, use the <see cref="GetDatabase(string, string, string)"/> overload
    /// or set the keyspace on the <see cref="GetDatabaseOptions"/> parameter and use the <see cref="GetDatabase(string, GetDatabaseOptions)"/> overload.
    /// </summary>
    /// <param name="apiEndpoint">The API endpoint of the database.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataAPIClient("token");
    /// var database = client.GetDatabase("https://01234567-89ab-cdef-0123-456789abcdef-us-east1.apps.astra.datastax.com");
    /// </code>
    /// </example>
    public Database GetDatabase(string apiEndpoint)
    {
        return GetDatabase(apiEndpoint, null as GetDatabaseOptions);
    }

    /// <summary>
    /// Gets an instance of the <see cref="Database"/> class given the API Endpoint of the database to connect to, a token to use, and optionally a keyspace.
    /// If the keyspace is not provided the default keyspace will be used.
    /// </summary>
    /// <param name="apiEndpoint">The API endpoint of the database.</param>
    /// <param name="token">The specific token to use for this database connection.</param>
    /// <param name="keyspace">Optional: The keyspace to connect to.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataAPIClient();
    /// var database = client.GetDatabase("https://01234567-89ab-cdef-0123-456789abcdef-us-east1.apps.astra.datastax.com", "token", "myKeyspace");
    /// </code>
    /// </example>
    public Database GetDatabase(string apiEndpoint, string token, string keyspace = null)
    {
        var options = new GetDatabaseOptions() { Token = token, Keyspace = keyspace };
        return GetDatabase(apiEndpoint, options);
    }

    /// <summary>
    /// Gets an instance of a <see cref="Database"/> given the API Endpoint and a set of options.
    /// 
    /// Any options provided in the <paramref name="options"/> parameter will take precedence over the options from the <see cref="DataAPIClient"/>.
    /// </summary>
    /// <param name="apiEndpoint">The API endpoint of the database.</param>
    /// <param name="options">The options to use for the database, optionally including token or keyspace.</param>
    /// <returns>An instance of the <see cref="Database"/> class.</returns>
    /// <example>
    /// <code>
    /// var client = new DataAPIClient("token");
    /// var database = client.GetDatabase("https://01234567-89ab-cdef-0123-456789abcdef-us-east1.apps.astra.datastax.com", new DatabaseCommandOptions() { Keyspace = "myKeyspace" });
    /// </code>
    /// </example>
    public Database GetDatabase(string apiEndpoint, GetDatabaseOptions options)
    {
        return new Database(apiEndpoint, this, options);
    }

    /// <summary>
    /// Generate a db token given a username and password (generally needed for instances other than Astra DB)
    /// </summary>
    /// <param name="username"></param>
    /// <param name="password"></param>
    /// <returns></returns>
    public static string UsernamePasswordTokenProvider(string username, string password)
    {
        return $"Cassandra:{Base64Encode(username)}:{Base64Encode(password)}";
    }

    internal static string Base64Encode(string input)
    {
        byte[] stringBytes = System.Text.Encoding.UTF8.GetBytes(input);
        return System.Convert.ToBase64String(stringBytes);
    }

}
